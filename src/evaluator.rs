use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use url::Url;

use muzanci_config::Config;
use muzanci_config::config::TriggerConfig;
use muzanci_git::GitBranch;
use muzanci_git::GitClient;
use muzanci_git::GitCommitSha;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::EvaluatorMessage;
use muzanci_transport::message::Message;
use muzanci_transport::message::TriggerId;

use crate::RunnerState;
use crate::evaluation_capacity::EvaluationCapacity;
use crate::evaluation_capacity::EvaluationCapacityPermit;

pub struct EvaluatorHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for EvaluatorHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct Evaluator {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    trigger_id: TriggerId,
    capacity: EvaluationCapacity,
    _permit: EvaluationCapacityPermit,
}

impl Evaluator {
    pub fn spawn(
        runner_state: Arc<RunnerState>,
        trigger_id: TriggerId,
        capacity: EvaluationCapacity,
        permit: EvaluationCapacityPermit,
    ) -> EvaluatorHandle {
        let runner_state = runner_state.clone();
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx) = runner_state
                .mux_handle
                .open_channel(ChannelType::Evaluator)
                .await
                .unwrap();
            Evaluator {
                runner_state,
                channel_tx,
                channel_rx,
                trigger_id,
                capacity,
                _permit: permit,
            }
            .run()
            .await
            .unwrap();
        });
        EvaluatorHandle { handle }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Evaluator received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                result
            }
        }
    }

    async fn main(&mut self) -> anyhow::Result<()> {
        let config = self.start().await?;
        match self.evaluate(&config).await {
            Ok(config) => self.complete(config).await,
            Err(e) => self.fail(e.to_string()).await,
        }
    }

    async fn start(&mut self) -> anyhow::Result<TriggerConfig> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::StartRequest {
                runner_id: self.runner_state.runner_id,
                trigger_id: self.trigger_id,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Evaluator(EvaluatorMessage::StartResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn evaluate(&self, config: &TriggerConfig) -> anyhow::Result<Config> {
        let evaluator_dir = tempfile::tempdir_in(&self.runner_state.evaluator_dir_root)?;

        {
            let git_client = GitClient::try_default()?;
            git_client.checkout_commit(
                &config.remote.url,
                &config.remote.branch,
                &evaluator_dir.path(),
                &config.commit_sha,
            )?;
            // git_client must be dropped here because it is not Send.
            // TODO: Consider offloading to a tokio::task::spawn_blocking.
        }

        let input = evaluator_dir.path().join(&config.input);
        Config::from_file(&input, &HashMap::new())
    }

    async fn complete(&mut self, config: Config) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::CompleteRequest {
                runner_id: self.runner_state.runner_id,
                trigger_id: self.trigger_id,
                config,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Evaluator(EvaluatorMessage::CompleteResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn fail(&mut self, reason: String) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::FailRequest {
                runner_id: self.runner_state.runner_id,
                trigger_id: self.trigger_id,
                reason,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Evaluator(EvaluatorMessage::FailResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }
}

impl Drop for Evaluator {
    fn drop(&mut self) {
        self.runner_state
            .shared_evaluation_capacity
            .restore(self.capacity);
    }
}
