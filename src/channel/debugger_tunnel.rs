use std::sync::Arc;

use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::combine_into_byte_stream;
use muzanci_transport::message::DebuggerTunnelMessage;
use muzanci_transport::message::Message;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use muzanci_config::config::DebugSessionId;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::mux::MuxHandle;

use crate::ssh::server::ServerHandler;

pub struct DebuggerTunnelHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for DebuggerTunnelHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct DebuggerTunnel {
    cancellation_token: CancellationToken,
    debug_session_id: DebugSessionId,
    channel_tx: ChannelSender,
}

impl DebuggerTunnel {
    pub fn spawn(
        mux_handle: MuxHandle,
        cancellation_token: CancellationToken,
        debug_session_id: DebugSessionId,
        reply_tx: oneshot::Sender<()>,
    ) -> DebuggerTunnelHandle {
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx, channel_closed) = mux_handle
                .open_channel(ChannelType::DebuggerTunnel)
                .await
                .unwrap();
            DebuggerTunnel {
                cancellation_token,
                debug_session_id,
                channel_tx,
            }
            .run(channel_rx, channel_closed, reply_tx)
            .await
            .unwrap();
        });
        DebuggerTunnelHandle { handle }
    }

    #[tracing::instrument(skip_all)]
    async fn run(
        self,
        channel_rx: ChannelReceiver,
        channel_closed: CancellationToken,
        reply_tx: oneshot::Sender<()>,
    ) -> anyhow::Result<()> {
        let cancellation_token = self.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("DebuggerTunnel received cancellation signal.");
                Ok(())
            }

            _ = channel_closed.cancelled() => {
                tracing::info!("Channel closed. Stopping DebuggerTunnel.");
                Ok(())
            }

            result = self.main(channel_rx, reply_tx) => {
                match result {
                    Ok(_) => {
                        tracing::info!("DebuggerTunnel finished running.");
                    }
                    Err(e) => {
                        tracing::error!("DebuggerTunnel encountered an error: {:?}", e);
                    }
                }
                Ok(())
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn main(
        self,
        mut channel_rx: ChannelReceiver,
        reply_tx: oneshot::Sender<()>,
    ) -> anyhow::Result<()> {
        self.create_debug_tunnel(&mut channel_rx).await?;
        tracing::info!("Created debug tunnel");

        let session = self.start_ssh_server(channel_rx).await?;
        tracing::info!("Started SSH server");

        let _ = reply_tx.send(());
        tracing::info!("Sent reply");

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn create_debug_tunnel(&self, channel_rx: &mut ChannelReceiver) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebuggerTunnel(
                DebuggerTunnelMessage::CreateDebugTunnelRequest {
                    debug_session_id: self.debug_session_id,
                },
            ))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebuggerTunnel(DebuggerTunnelMessage::CreateDebugTunnelResponse {
                    result,
                }) => result.map_err(|e| anyhow::anyhow!(e)),
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn start_ssh_server(&self, channel_rx: ChannelReceiver) -> anyhow::Result<()> {
        let private_key = russh::keys::PrivateKey::random(
            &mut russh::keys::key::safe_rng(),
            russh::keys::Algorithm::Ed25519,
        )?;
        let config = Arc::new(russh::server::Config {
            keys: vec![private_key],
            ..Default::default()
        });

        let stream = {
            let channel_tx = self.channel_tx.clone();
            combine_into_byte_stream(channel_tx, channel_rx)
        };

        let server_handler = ServerHandler::new("jid".to_string());

        tokio::spawn(russh::server::run_stream(config, stream, server_handler));
        tracing::info!("SSH server started");

        Ok(())
    }
}
