use std::sync::Arc;

use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::EvaluatorSchedulerMessage;
use muzanci_transport::message::Message;
use muzanci_transport::message::TriggerId;
use muzanci_transport::message::WaitingTrigger;
use tokio_util::sync::CancellationToken;

use crate::RunnerState;
use crate::channel::evaluator::Evaluator;

pub struct EvaluatorSchedulerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for EvaluatorSchedulerHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct EvaluatorScheduler {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
}

impl EvaluatorScheduler {
    pub fn spawn(runner_state: Arc<RunnerState>) -> EvaluatorSchedulerHandle {
        let runner_state = runner_state.clone();
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx, channel_closed) = runner_state
                .mux_handle
                .open_channel(ChannelType::EvaluatorScheduler)
                .await
                .unwrap();
            EvaluatorScheduler {
                runner_state,
                channel_tx,
            }
            .run(channel_rx, channel_closed)
            .await
            .unwrap();
        });
        EvaluatorSchedulerHandle { handle }
    }

    async fn run(
        self,
        channel_rx: ChannelReceiver,
        channel_closed: CancellationToken,
    ) -> anyhow::Result<()> {
        tracing::info!("EvaluatorScheduler started running.");
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("EvaluatorScheduler received cancellation signal.");
                Ok(())
            }

            _ = channel_closed.cancelled() => {
                tracing::info!("Channel closed. Stopping EvaluatorScheduler.");
                Ok(())
            }

            result = self.main(channel_rx) => {
                match result {
                    Ok(_) => {
                        tracing::info!("EvaluatorScheduler finished running.");
                    }
                    Err(e) => {
                        tracing::error!("EvaluatorScheduler encountered an error: {:?}", e);
                    }
                }
                Ok(())
            }
        }
    }

    async fn main(self, mut channel_rx: ChannelReceiver) -> anyhow::Result<()> {
        loop {
            let triggers = self.fetch_waiting_triggers(&mut channel_rx).await?;

            // Iterate over triggers and attempt to reserve until capacity is reached or no more triggers are available.
            for trigger in triggers {
                let permit = match self
                    .runner_state
                    .shared_evaluation_capacity
                    .reserve(trigger.capacity)
                    .await
                {
                    Ok(permit) => permit,
                    Err(e) => {
                        tracing::error!("Failed to reserve capacity {:?}: {:?}", trigger, e);
                        continue;
                    }
                };

                match self
                    .reserve_trigger(&mut channel_rx, trigger.trigger_id)
                    .await
                {
                    Ok(()) => {
                        tracing::info!("Successfully reserved trigger {:?}", trigger);
                        Evaluator::spawn(
                            self.runner_state.clone(),
                            trigger.trigger_id,
                            trigger.capacity,
                            permit,
                        );
                    }
                    Err(e) => {
                        tracing::error!("Failed to reserve trigger {:?}: {:?}", trigger, e);
                        drop(permit);
                    }
                }
            }

            // TODO: Fix bug where scheduler does not check server again, even if capacity is available.
            // Wait for notification of available capacity before checking for triggers again.
            self.runner_state
                .shared_evaluation_capacity
                .notified()
                .await;
        }
    }

    // TODO: Add filters for waiting triggers.
    async fn fetch_waiting_triggers(
        &self,
        channel_rx: &mut ChannelReceiver,
    ) -> anyhow::Result<Vec<WaitingTrigger>> {
        tracing::info!("Fetching waiting triggers from the server.");
        self.channel_tx
            .send(Message::EvaluatorScheduler(
                EvaluatorSchedulerMessage::FetchWaitingTriggersRequest,
            ))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::EvaluatorScheduler(
                    EvaluatorSchedulerMessage::FetchWaitingTriggersResponse { result },
                ) => result.map_err(|e| anyhow::anyhow!(e)),
                _ => {
                    tracing::error!("Unexpected response: {:?}", response);
                    Err(anyhow::anyhow!("Unexpected response"))
                }
            })
    }

    // Uses the reserve and commit pattern for cancellation safety.
    async fn reserve_trigger(
        &self,
        channel_rx: &mut ChannelReceiver,
        trigger_id: TriggerId,
    ) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::EvaluatorScheduler(
                EvaluatorSchedulerMessage::ReserveTriggerRequest {
                    runner_id: self.runner_state.runner_id,
                    trigger_id,
                },
            ))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::EvaluatorScheduler(
                    EvaluatorSchedulerMessage::ReserveTriggerResponse { result },
                ) => result.map_err(|e| anyhow::anyhow!(e)),
                _ => {
                    tracing::error!("Unexpected response: {:?}", response);
                    Err(anyhow::anyhow!("Unexpected response"))
                }
            })
    }
}
