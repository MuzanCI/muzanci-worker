use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
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
            let (channel_tx, channel_rx, channel_closed) = runner_state
                .mux_handle
                .open_channel(ChannelType::Evaluator)
                .await
                .unwrap();
            Evaluator {
                runner_state,
                channel_tx,
                trigger_id,
                capacity,
                _permit: permit,
            }
            .run(channel_rx, channel_closed)
            .await
            .unwrap();
        });
        EvaluatorHandle { handle }
    }

    async fn run(
        self,
        channel_rx: ChannelReceiver,
        channel_closed: CancellationToken,
    ) -> anyhow::Result<()> {
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Evaluator received cancellation signal.");
                Ok(())
            }

            _ = channel_closed.cancelled() => {
                tracing::info!("Channel closed. Stopping Evaluator.");
                Ok(())
            }

            result = self.main(channel_rx) => {
                result
            }
        }
    }

    async fn main(self, mut channel_rx: ChannelReceiver) -> anyhow::Result<()> {
        let config = self.start(&mut channel_rx).await?;
        match self.evaluate(&config).await {
            Ok(config) => self.complete(&mut channel_rx, config).await,
            Err(e) => self.fail(&mut channel_rx, e.to_string()).await,
        }
    }

    async fn start(&self, channel_rx: &mut ChannelReceiver) -> anyhow::Result<TriggerConfig> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::StartRequest {
                runner_id: self.runner_state.runner_id,
                trigger_id: self.trigger_id,
            }))
            .await?;

        channel_rx
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

        tracing::info!(
            "checked out {}/{}@{:?}",
            config.remote.url,
            config.remote.branch,
            config.commit_sha
        );

        let input = evaluator_dir.path().join(&config.input);
        let mut env = HashMap::new();
        env.insert("GIT_BRANCH".to_string(), config.remote.branch.clone());
        env.insert("GIT_COMMIT".to_string(), config.commit_sha.clone());
        Config::from_file(&input, &env)
    }

    async fn complete(
        &self,
        channel_rx: &mut ChannelReceiver,
        config: Config,
    ) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::CompleteRequest {
                runner_id: self.runner_state.runner_id,
                trigger_id: self.trigger_id,
                config,
            }))
            .await?;

        channel_rx
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

    async fn fail(&self, channel_rx: &mut ChannelReceiver, reason: String) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::FailRequest {
                runner_id: self.runner_state.runner_id,
                trigger_id: self.trigger_id,
                reason,
            }))
            .await?;

        channel_rx
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
