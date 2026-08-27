use std::sync::Arc;

use muzanci_config::config::DebugSessionId;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::DebuggerSchedulerMessage;
use muzanci_transport::message::Message;
use muzanci_transport::message::WaitingDebugSession;

use crate::RunnerState;
use crate::debugger::Debugger;

pub struct DebuggerSchedulerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for DebuggerSchedulerHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct DebuggerScheduler {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
}

impl DebuggerScheduler {
    pub fn spawn(runner_state: Arc<RunnerState>) -> DebuggerSchedulerHandle {
        let runner_state = runner_state.clone();
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx) = runner_state
                .mux_handle
                .open_channel(ChannelType::DebuggerScheduler)
                .await
                .unwrap();
            DebuggerScheduler {
                runner_state,
                channel_tx,
                channel_rx,
            }
            .run()
            .await
            .unwrap();
        });
        DebuggerSchedulerHandle { handle }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        tracing::info!("DebuggerScheduler started running.");
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("DebuggerScheduler received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                match result {
                    Ok(_) => {
                        tracing::info!("DebuggerScheduler finished running.");
                    }
                    Err(e) => {
                        tracing::error!("DebuggerScheduler encountered an error: {:?}", e);
                    }
                }
                Ok(())
            }
        }
    }

    async fn main(&mut self) -> anyhow::Result<()> {
        loop {
            let debugs = self.fetch_waiting_debugs().await?;

            // Iterate over debugs and attempt to reserve until capacity is reached or no more debugs are available.
            for waiting_debug in debugs {
                let capacity = 1;
                let permit = match self
                    .runner_state
                    .shared_assignment_capacity_handle
                    .reserve_high(capacity)
                    .await
                {
                    Ok(permit) => permit,
                    Err(e) => {
                        tracing::error!("Failed to reserve capacity {:?}: {:?}", capacity, e);
                        continue;
                    }
                };
                match self.reserve_debug(waiting_debug.debug_session_id).await {
                    Ok(_) => {
                        tracing::info!("Successfully reserved debug {:?}", waiting_debug);
                        Debugger::spawn(
                            self.runner_state.clone(),
                            waiting_debug.debug_session_id,
                            permit,
                        );
                    }
                    Err(e) => {
                        tracing::error!("Failed to reserve debug {:?}: {:?}", waiting_debug, e);
                        drop(permit);
                    }
                }
            }

            // Wait for notification of available capacity before checking for waiting debugs again.
            tracing::info!(
                "Waiting for available capacity before checking for waiting debugs again."
            );
            // TODO: Fix bug where scheduler does not check server again, even if capacity is available.
            self.runner_state
                .shared_assignment_capacity_handle
                .notified()
                .await;
        }
    }

    // TODO: Add filters for waiting debugs.
    async fn fetch_waiting_debugs(&mut self) -> anyhow::Result<Vec<WaitingDebugSession>> {
        tracing::info!("Fetching waiting debugs from the server.");
        self.channel_tx
            .send(Message::DebuggerScheduler(
                DebuggerSchedulerMessage::FetchWaitingDebugSessionsRequest,
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebuggerScheduler(
                    DebuggerSchedulerMessage::FetchWaitingDebugSessionsResponse { result },
                ) => result.map_err(|e| anyhow::anyhow!(e)),
                _ => {
                    tracing::error!("Unexpected response: {:?}", response);
                    Err(anyhow::anyhow!("Unexpected response"))
                }
            })
    }

    // Uses the reserve and commit pattern for cancellation safety.
    async fn reserve_debug(&mut self, debug_session_id: DebugSessionId) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebuggerScheduler(
                DebuggerSchedulerMessage::ReserveDebugSessionRequest {
                    runner_id: self.runner_state.runner_id,
                    debug_session_id,
                },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebuggerScheduler(
                    DebuggerSchedulerMessage::ReserveDebugSessionResponse { result },
                ) => result.map_err(|e| anyhow::anyhow!(e)),
                _ => {
                    eprintln!("Unexpected response: {:?}", response);
                    Err(anyhow::anyhow!("Unexpected response"))
                }
            })
    }
}
