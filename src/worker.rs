use std::collections::HashMap;
use std::sync::Arc;
use tokio::join;
use tokio::sync::mpsc;

use muzanci_config::StepConfig;
use muzanci_config::StepId;
use muzanci_git::GitClient;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::ExitStatus;
use muzanci_transport::message::Message;
use muzanci_transport::message::ProcessOutput;
use muzanci_transport::message::TaskConfig;
use muzanci_transport::message::TaskId;
use muzanci_transport::message::WorkerMessage;

use crate::RunnerState;
use crate::assignment_capacity::AssignmentCapacityPermit;
use crate::sandbox::Sandbox;
use crate::sandbox::SandboxConfig;
use crate::sandbox::SandboxId;

pub struct WorkerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for WorkerHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct Worker {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    task_id: TaskId,
    _permit: AssignmentCapacityPermit,
}

enum ItemResult {
    Continue,
    Fail(String),
}

impl Worker {
    pub fn spawn(
        runner_state: Arc<RunnerState>,
        task_id: TaskId,
        permit: AssignmentCapacityPermit,
    ) -> WorkerHandle {
        let runner_state = runner_state.clone();
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx) = runner_state
                .mux_handle
                .open_channel(ChannelType::Worker)
                .await
                .unwrap();
            Worker {
                runner_state,
                channel_tx,
                channel_rx,
                task_id,
                _permit: permit,
            }
            .run()
            .await
            .unwrap();
        });
        WorkerHandle { handle }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                eprintln!("Worker received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                result
            }
        }
    }

    async fn main(&mut self) -> anyhow::Result<()> {
        let task_config = self.start_task().await?;
        let sandbox_config = SandboxConfig {
            sandbox_id: SandboxId::now_v7(),
            image: task_config.image,
        };
        let sandbox: Arc<dyn Sandbox> = {
            let sandbox = self.runner_state.sandboxer.create(sandbox_config).await?;
            Arc::from(sandbox)
        };
        {
            let git_client = GitClient::try_default()?;
            git_client.checkout_commit(
                &task_config.remote.url,
                &task_config.remote.branch,
                &sandbox.workspace_path(),
                &task_config.commit_sha,
            )?;
        }
        for step in task_config.steps {
            match self.run_item(sandbox.clone(), step).await? {
                ItemResult::Continue => {
                    continue;
                }
                ItemResult::Fail(reason) => {
                    self.fail_task(reason).await?;
                    // The step failed so we stop, but the worker itself
                    // is considered successful.
                    return Ok(());
                }
            }
        }
        self.complete_task().await
    }

    async fn start_task(&mut self) -> anyhow::Result<TaskConfig> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::StartTaskRequest {
                runner_id: self.runner_state.runner_id,
                task_id: self.task_id,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::StartTaskResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn run_item(
        &mut self,
        sandbox: Arc<dyn Sandbox>,
        step: StepConfig,
    ) -> anyhow::Result<ItemResult> {
        let step_id = step.step_id;
        self.start_item(step_id).await?;

        let envs = {
            let mut envs = HashMap::new();
            envs
        };

        let exit_status = {
            let (output_tx, output_rx) = mpsc::channel(1);
            let output_handle = WorkerItemOutput::spawn(
                self.runner_state.clone(),
                self.channel_tx.clone(),
                self.task_id,
                step_id,
                output_rx,
            );
            let process_handle = sandbox.run(&step.command, &envs, output_tx);
            let (process_result, _output_result) = join!(process_handle, output_handle);

            match process_result?.code() {
                Some(code) => ExitStatus::Code(code),
                None => ExitStatus::Signal,
            }
        };

        self.channel_tx
            .send(Message::Worker(WorkerMessage::ItemProcessExitStatus {
                runner_id: self.runner_state.runner_id,
                task_id: self.task_id,
                step_id,
                exit_status,
            }))
            .await?;

        match exit_status {
            ExitStatus::Code(code) if code == 0 => {
                self.complete_item(step_id).await?;
                Ok(ItemResult::Continue)
            }
            ExitStatus::Code(code) => {
                self.fail_item(
                    step_id,
                    format!("Process exited with non-zero status code: [{}]", code),
                )
                .await?;
                Ok(ItemResult::Fail(format!(
                    "Process exited with non-zero status code: [{}]",
                    code
                )))
            }
            ExitStatus::Signal => {
                self.fail_item(step_id, "Process terminated by signal".to_string())
                    .await?;
                Ok(ItemResult::Fail("Process terminated by signal".to_string()))
            }
        }
    }

    async fn complete_task(&mut self) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::CompleteTaskRequest {
                runner_id: self.runner_state.runner_id,
                task_id: self.task_id,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::CompleteTaskResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn fail_task(&mut self, reason: String) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::FailTaskRequest {
                runner_id: self.runner_state.runner_id,
                task_id: self.task_id,
                reason,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::FailTaskResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn start_item(&mut self, step_id: StepId) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::StartItemRequest {
                runner_id: self.runner_state.runner_id,
                task_id: self.task_id,
                step_id,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::StartItemResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn complete_item(&mut self, step_id: StepId) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::CompleteItemRequest {
                runner_id: self.runner_state.runner_id,
                task_id: self.task_id,
                step_id,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::CompleteItemResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn fail_item(&mut self, step_id: StepId, reason: String) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::FailItemRequest {
                runner_id: self.runner_state.runner_id,
                task_id: self.task_id,
                step_id,
                reason,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::FailItemResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }
}

pub struct WorkerStepOutputHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for WorkerStepOutputHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct WorkerItemOutput {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    task_id: TaskId,
    step_id: StepId,
    output_rx: mpsc::Receiver<ProcessOutput>,
}

impl WorkerItemOutput {
    pub fn spawn(
        runner_state: Arc<RunnerState>,
        channel_tx: ChannelSender,
        task_id: TaskId,
        step_id: StepId,
        output_rx: mpsc::Receiver<ProcessOutput>,
    ) -> WorkerStepOutputHandle {
        let runner_state = runner_state.clone();
        let handle = tokio::spawn(async move {
            WorkerItemOutput {
                runner_state,
                channel_tx,
                task_id,
                step_id,
                output_rx,
            }
            .run()
            .await
            .unwrap();
        });
        WorkerStepOutputHandle { handle }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                eprintln!("WorkerStepOutput received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                result
            }
        }
    }

    async fn main(&mut self) -> anyhow::Result<()> {
        while let Some(output) = self.output_rx.recv().await {
            let result = self
                .channel_tx
                .send(Message::Worker(WorkerMessage::ItemProcessOutput {
                    runner_id: self.runner_state.runner_id,
                    task_id: self.task_id,
                    step_id: self.step_id,
                    output,
                }))
                .await;

            if let Err(e) = result {
                tracing::error!("Failed to send worker step process output: {}", e);
                anyhow::bail!("Failed to send worker step process output: {}", e);
            }
        }
        Ok(())
    }
}
