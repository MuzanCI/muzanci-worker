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
    channel_tx: Option<ChannelSender>,
    channel_rx: Option<ChannelReceiver>,
}

impl DebuggerTunnel {
    pub fn spawn(
        mux_handle: MuxHandle,
        cancellation_token: CancellationToken,
        debug_session_id: DebugSessionId,
        reply_tx: oneshot::Sender<()>,
    ) -> DebuggerTunnelHandle {
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx) = mux_handle
                .open_channel(ChannelType::DebuggerTunnel)
                .await
                .unwrap();
            DebuggerTunnel {
                cancellation_token,
                debug_session_id,
                channel_tx: Some(channel_tx),
                channel_rx: Some(channel_rx),
            }
            .run(reply_tx)
            .await
            .unwrap();
        });
        DebuggerTunnelHandle { handle }
    }

    #[tracing::instrument(skip_all)]
    async fn run(&mut self, reply_tx: oneshot::Sender<()>) -> anyhow::Result<()> {
        let cancellation_token = self.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("DebuggerTunnel received cancellation signal.");
                Ok(())
            }

            result = self.main(reply_tx) => {
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
    async fn main(&mut self, reply_tx: oneshot::Sender<()>) -> anyhow::Result<()> {
        self.create_debug_tunnel().await?;
        tracing::info!("Created debug tunnel");
        let session = self.start_ssh_server().await?;
        tracing::info!("Started SSH server");
        let _ = reply_tx.send(());
        tracing::info!("Sent reply");
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn create_debug_tunnel(&mut self) -> anyhow::Result<()> {
        let channel_tx = self
            .channel_tx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("channel_tx is not set"))?;

        let channel_rx = self
            .channel_rx
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("channel_rx is not set"))?;

        channel_tx
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
    async fn start_ssh_server(&mut self) -> anyhow::Result<()> {
        let private_key = russh::keys::PrivateKey::random(
            &mut russh::keys::key::safe_rng(),
            russh::keys::Algorithm::Ed25519,
        )?;
        let config = Arc::new(russh::server::Config {
            keys: vec![private_key],
            ..Default::default()
        });
        let stream = {
            let channel_tx = self
                .channel_tx
                .take()
                .ok_or_else(|| anyhow::anyhow!("channel_tx is not set"))?;
            let channel_rx = self
                .channel_rx
                .take()
                .ok_or_else(|| anyhow::anyhow!("channel_rx is not set"))?;
            combine_into_byte_stream(channel_tx, channel_rx)
        };

        let server_handler = ServerHandler::new("jid".to_string());

        tracing::info!("About to start SSH server");
        tokio::spawn(russh::server::run_stream(config, stream, server_handler));

        tracing::info!("SSH server started");

        Ok(())
    }
}
