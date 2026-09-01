use std::sync::Arc;

use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::Message;
use muzanci_transport::message::TaskId;
use muzanci_transport::message::WaitingTask;
use muzanci_transport::message::WorkerSchedulerMessage;
use tokio_util::sync::CancellationToken;

use crate::RunnerState;
use crate::channel::worker::Worker;

pub struct WorkerSchedulerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for WorkerSchedulerHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct WorkerScheduler {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
}

impl WorkerScheduler {
    pub fn spawn(runner_state: Arc<RunnerState>) -> WorkerSchedulerHandle {
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx, channel_closed) = runner_state
                .mux_handle
                .open_channel(ChannelType::WorkerScheduler)
                .await
                .unwrap();
            WorkerScheduler {
                runner_state,
                channel_tx,
            }
            .run(channel_rx, channel_closed)
            .await
            .unwrap();
        });
        WorkerSchedulerHandle { handle }
    }

    async fn run(
        self,
        channel_rx: ChannelReceiver,
        channel_closed: CancellationToken,
    ) -> anyhow::Result<()> {
        tracing::info!("WorkerScheduler started running.");
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("WorkerScheduler received cancellation signal. Stopping WorkerScheduler.");
                Ok(())
            }

            _ = channel_closed.cancelled() => {
                tracing::info!("Channel closed while scheduling workers. Stopping WorkerScheduler.");
                Ok(())
            }

            result = self.main(channel_rx) => {
                match result {
                    Ok(_) => {
                        tracing::info!("WorkerScheduler finished running.");
                    }
                    Err(e) => {
                        tracing::error!("WorkerScheduler encountered an error: {:?}", e);
                    }
                }
                Ok(())
            }
        }
    }

    async fn main(self, mut channel_rx: ChannelReceiver) -> anyhow::Result<()> {
        loop {
            let tasks = self.fetch_waiting_tasks(&mut channel_rx).await?;

            // Iterate over tasks and attempt to reserve until capacity is reached or no more tasks are available.
            for task in tasks {
                let permit = match self
                    .runner_state
                    .shared_assignment_capacity_handle
                    .reserve_low(task.capacity)
                    .await
                {
                    Ok(permit) => permit,
                    Err(e) => {
                        tracing::error!("Failed to reserve capacity {:?}: {:?}", task.capacity, e);
                        continue;
                    }
                };
                match self.reserve_task(&mut channel_rx, task.task_id).await {
                    Ok(_) => {
                        tracing::info!("Successfully reserved task {:?}", task);
                        Worker::spawn(self.runner_state.clone(), task.task_id, permit);
                    }
                    Err(e) => {
                        tracing::error!("Failed to reserve task {:?}: {:?}", task, e);
                        drop(permit);
                    }
                }
            }

            // TODO: Fix bug where scheduler does not check server again, even if capacity is available.

            // Wait for notification of available capacity before checking for tasks again.
            self.runner_state
                .shared_assignment_capacity_handle
                .notified()
                .await;
        }
    }

    // TODO: Add filters for waiting tasks.
    async fn fetch_waiting_tasks(
        &self,
        channel_rx: &mut ChannelReceiver,
    ) -> anyhow::Result<Vec<WaitingTask>> {
        tracing::info!("Fetching waiting tasks from the server.");
        self.channel_tx
            .send(Message::WorkerScheduler(
                WorkerSchedulerMessage::FetchWaitingTasksRequest,
            ))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::WorkerScheduler(WorkerSchedulerMessage::FetchWaitingTasksResponse {
                    result,
                }) => result.map_err(|e| anyhow::anyhow!(e)),
                _ => {
                    tracing::error!("Unexpected response: {:?}", response);
                    Err(anyhow::anyhow!("Unexpected response"))
                }
            })
    }

    // Uses the reserve and commit pattern for cancellation safety.
    async fn reserve_task(
        &self,
        channel_rx: &mut ChannelReceiver,
        task_id: TaskId,
    ) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::WorkerScheduler(
                WorkerSchedulerMessage::ReserveTaskRequest {
                    runner_id: self.runner_state.runner_id,
                    task_id,
                },
            ))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::WorkerScheduler(WorkerSchedulerMessage::ReserveTaskResponse {
                    result,
                }) => result.map_err(|e| anyhow::anyhow!(e)),
                _ => {
                    eprintln!("Unexpected response: {:?}", response);
                    Err(anyhow::anyhow!("Unexpected response"))
                }
            })
    }
}
