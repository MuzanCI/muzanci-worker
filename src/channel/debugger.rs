use muzanci_config::config::ImageConfig;
use sha2::Digest;
use sha2::Sha256;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use url::Url;

use muzanci_config::StepConfig;
use muzanci_config::config::DebugSessionId;
use muzanci_git::GitBranch;
use muzanci_git::GitClient;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::DebugClientMessage;
use muzanci_transport::message::DebuggerMessage;
use muzanci_transport::message::Message;

use crate::RunnerState;
use crate::assignment_capacity::AssignmentCapacityPermit;
use crate::channel::debugger_tunnel::DebuggerTunnel;
use crate::sandbox::Sandbox;
use crate::sandbox::SandboxConfig;
use crate::sandbox::SandboxId;

#[derive(thiserror::Error, Debug)]
#[error("{0}")]
pub struct DebuggerError(String);

pub struct DebuggerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for DebuggerHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct Debugger {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    debug_session_id: DebugSessionId,
    sandbox: Option<Arc<dyn Sandbox>>,
    diff_file: Option<NamedTempFile>,
    diff_hasher: Option<Sha256>,
    _permit: AssignmentCapacityPermit,
}

impl Debugger {
    pub fn spawn(
        runner_state: Arc<RunnerState>,
        debug_session_id: DebugSessionId,
        permit: AssignmentCapacityPermit,
    ) -> DebuggerHandle {
        let runner_state = runner_state.clone();
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx, channel_closed) = runner_state
                .mux_handle
                .open_channel(ChannelType::Debugger)
                .await
                .unwrap();
            Debugger {
                runner_state,
                channel_tx,
                debug_session_id,
                sandbox: None,
                diff_file: None,
                diff_hasher: None,
                _permit: permit,
            }
            .run(channel_rx, channel_closed)
            .await
            .unwrap();
        });
        DebuggerHandle { handle }
    }

    #[tracing::instrument(skip_all)]
    async fn run(
        self,
        channel_rx: ChannelReceiver,
        channel_closed: CancellationToken,
    ) -> anyhow::Result<()> {
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("Debugger received cancellation signal.");
                Ok(())
            }
            _ = channel_closed.cancelled() => {
                tracing::info!("Channel closed, stopping Debugger.");
                Ok(())
            }
            result = self.main(channel_rx) => {
                result
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn main(mut self, mut channel_rx: ChannelReceiver) -> anyhow::Result<()> {
        self.connect_debugger(&mut channel_rx).await?;
        loop {
            match channel_rx.recv().await {
                Some(message) => {
                    self.handle_message(message).await?;
                }
                None => {
                    tracing::info!("Channel closed, stopping Debugger.");
                    return Ok(());
                }
            }
        }
    }

    async fn connect_debugger(&self, channel_rx: &mut ChannelReceiver) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Debugger(DebuggerMessage::ConnectDebuggerRequest {
                debug_session_id: self.debug_session_id,
            }))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Debugger(DebuggerMessage::ConnectDebuggerResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn handle_message(&mut self, message: Message) -> anyhow::Result<()> {
        let message = match message {
            Message::DebugClient(msg) => msg,
            _ => Err(DebuggerError(format!("unexpected message: {:?}", message)))?,
        };

        match message {
            DebugClientMessage::CreateSandboxRequest { image } => {
                self.handle_create_sandbox_request(image).await
            }
            DebugClientMessage::CheckoutBranchRequest { url, branch } => {
                self.handle_checkout_branch_request(url, branch).await
            }
            DebugClientMessage::StartDiffUploadRequest => {
                self.handle_start_diff_upload_request().await
            }
            DebugClientMessage::UploadDiffChunkRequest { chunk } => {
                self.handle_upload_diff_chunk_request(chunk).await
            }
            DebugClientMessage::CompleteDiffUploadRequest { checksum } => {
                self.handle_complete_diff_upload_request(checksum).await
            }
            DebugClientMessage::ApplyDiffRequest => self.handle_apply_diff_request().await,
            DebugClientMessage::StartShellRequest { step } => {
                self.handle_start_shell_request(step).await
            }
            DebugClientMessage::ExecuteStepRequest { step } => {
                self.handle_execute_step_request(step).await
            }
            _ => Err(DebuggerError(format!("unexpected message: {:?}", message)))?,
        }
    }

    async fn handle_create_sandbox_request(&mut self, image: ImageConfig) -> anyhow::Result<()> {
        let result = self.create_sandbox(image).await.map_err(|e| e.to_string());

        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::CreateSandboxResponse { result },
            ))
            .await?;

        Ok(())
    }

    async fn create_sandbox(&mut self, image: ImageConfig) -> anyhow::Result<()> {
        let sandbox_config = SandboxConfig {
            sandbox_id: SandboxId::now_v7(),
            image,
        };

        let sandbox = self.runner_state.sandboxer.create(sandbox_config).await?;

        tracing::info!(
            "Created sandbox at [{}]",
            sandbox.workspace_path().display()
        );

        self.sandbox = Some(sandbox);

        Ok(())
    }

    async fn handle_checkout_branch_request(
        &self,
        url: Url,
        branch: GitBranch,
    ) -> anyhow::Result<()> {
        let result = self
            .checkout_branch(url, branch)
            .await
            .map_err(|e| e.to_string());

        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::CheckoutBranchResponse { result },
            ))
            .await?;

        Ok(())
    }

    async fn checkout_branch(&self, url: Url, branch: GitBranch) -> anyhow::Result<()> {
        let sandbox = self
            .sandbox
            .as_ref()
            .ok_or_else(|| DebuggerError(format!("sandbox is None")))?;

        {
            let git_client = GitClient::try_default()?;
            tracing::info!(
                "Checking out repo [{}] branch [{}] to [{}]",
                url,
                branch,
                sandbox.workspace_path().display()
            );
            git_client.checkout_branch(&url, &branch, &sandbox.workspace_path())?;
        }

        Ok(())
    }

    async fn handle_start_diff_upload_request(&mut self) -> anyhow::Result<()> {
        let result = self.start_diff_upload().await.map_err(|e| e.to_string());

        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::StartDiffUploadResponse { result },
            ))
            .await?;

        Ok(())
    }

    async fn start_diff_upload(&mut self) -> anyhow::Result<()> {
        self.diff_file = Some(NamedTempFile::new()?);
        self.diff_hasher = Some(Sha256::new());
        Ok(())
    }

    async fn handle_upload_diff_chunk_request(&mut self, chunk: Vec<u8>) -> anyhow::Result<()> {
        let result = self
            .upload_diff_chunk(chunk)
            .await
            .map_err(|e| e.to_string());
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::UploadDiffChunkResponse { result },
            ))
            .await?;

        Ok(())
    }

    async fn upload_diff_chunk(&mut self, chunk: Vec<u8>) -> anyhow::Result<()> {
        {
            let diff_hasher = self
                .diff_hasher
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("diff upload has not been started"))?;
            diff_hasher.update(&chunk);
        }

        {
            let diff_file = self
                .diff_file
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("diff upload has not been started"))?;

            let file = diff_file.reopen()?;
            tokio::fs::File::from_std(file).write_all(&chunk).await?;
        }

        Ok(())
    }

    async fn handle_complete_diff_upload_request(
        &mut self,
        checksum: String,
    ) -> anyhow::Result<()> {
        let result = self
            .complete_diff_upload(checksum)
            .await
            .map_err(|e| e.to_string());
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::CompleteDiffUploadResponse { result },
            ))
            .await?;

        Ok(())
    }

    async fn complete_diff_upload(&mut self, checksum: String) -> anyhow::Result<()> {
        let diff_hasher = self
            .diff_hasher
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("diff upload has not been started"))?;
        let digest = hex::encode(diff_hasher.finalize_reset());
        tracing::info!("Diff digest [{}]", digest);
        if digest != checksum {
            anyhow::bail!("diff file checksum mismatch");
        }

        Ok(())
    }

    async fn handle_apply_diff_request(&self) -> anyhow::Result<()> {
        let result = self.apply_diff().await.map_err(|e| e.to_string());
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::ApplyDiffResponse { result },
            ))
            .await?;

        Ok(())
    }

    async fn apply_diff(&self) -> anyhow::Result<()> {
        let sandbox = self
            .sandbox
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("sandbox not available"))?;

        let diff_file = self
            .diff_file
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("diff file not available"))?;

        {
            let git_client = GitClient::try_default()?;
            git_client.apply_diff(diff_file.path(), &sandbox.workspace_path())?;
        }

        Ok(())
    }

    async fn handle_start_shell_request(&self, step: StepConfig) -> anyhow::Result<()> {
        let result = self.start_shell(step).await.map_err(|e| e.to_string());
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::StartShellResponse { result },
            ))
            .await?;

        Ok(())
    }

    async fn start_shell(&self, step: StepConfig) -> anyhow::Result<()> {
        let (reply_tx, reply_rx) = oneshot::channel();

        DebuggerTunnel::spawn(
            self.runner_state.mux_handle.clone(),
            self.runner_state.cancellation_token(),
            self.debug_session_id,
            reply_tx,
        );

        let () = reply_rx.await?;
        Ok(())
    }

    async fn handle_execute_step_request(&self, step: StepConfig) -> anyhow::Result<()> {
        let result = self.execute_step().await.map_err(|e| e.to_string());
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::ExecuteStepResponse { result },
            ))
            .await?;

        Ok(())
    }

    async fn execute_step(&self) -> anyhow::Result<()> {
        anyhow::bail!("not_implemented")
    }
}
