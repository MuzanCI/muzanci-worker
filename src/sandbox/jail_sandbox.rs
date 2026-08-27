use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitStatus;

use futures_util::StreamExt;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_util::codec::FramedRead;
use tokio_util::codec::LinesCodec;

use muzanci_image::image::ImagePlatformOs;
use muzanci_transport::message::ProcessOutput;

use crate::sandbox::Sandbox;
use crate::sandbox::SandboxConfig;
use crate::sandbox::SandboxError;
use crate::sandbox::jail_config::JailConfig;
use crate::sandbox::jail_slot::JailSlot;

pub struct JailSandbox {
    config: SandboxConfig,
    jail_conf: JailConfig,

    /// When dropped, the jail slot is automatically restored to [`FreeJailSlots`](crate::jail::jail_slot::FreeJailSlots).
    _slot: JailSlot,
}

impl JailSandbox {
    pub fn try_new(
        config: SandboxConfig,
        jail_conf: JailConfig,
        slot: JailSlot,
    ) -> Result<Self, SandboxError> {
        let sandbox = JailSandbox {
            config,
            jail_conf,
            _slot: slot,
        };
        std::fs::create_dir_all(&sandbox.workspace_path())
            .map_err(|e| SandboxError(e.to_string()))?;
        Ok(sandbox)
    }
}

#[async_trait::async_trait]
impl Sandbox for JailSandbox {
    fn config(&self) -> &SandboxConfig {
        &self.config
    }

    fn workspace_path(&self) -> PathBuf {
        match &self.config.image.platform.os {
            ImagePlatformOs::LINUX => self.jail_conf.path().join("compat/linux/workspace"),
            _ => self.jail_conf.path().join("workspace"),
        }
    }

    async fn run(
        &self,
        cmd_str: &str,
        envs: &HashMap<String, String>,
        output_tx: mpsc::Sender<ProcessOutput>,
    ) -> Result<ExitStatus, SandboxError> {
        let cmd_str = match &self.config.image.platform.os {
            ImagePlatformOs::LINUX => format!(
                "chroot /compat/linux sh -c \"cd {} && {}\"",
                self.workspace_path().display(),
                cmd_str
            ),
            ImagePlatformOs::FREEBSD => {
                format!("cd {} && {}", self.workspace_path().display(), cmd_str)
            }
            ImagePlatformOs::OTHER(os) => {
                return Err(SandboxError(format!("unsupported os [{}]", os)));
            }
        };

        let mut child = Command::new("sh")
            .arg("-c")
            .arg(format!("jexec {} {}", self.jail_conf.name(), cmd_str))
            .envs(envs)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| SandboxError(e.to_string()))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SandboxError("failed to take stdout".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| SandboxError("failed to take stderr".to_string()))?;

        let mut stdout_lines = FramedRead::new(stdout, LinesCodec::new());
        let mut stderr_lines = FramedRead::new(stderr, LinesCodec::new());

        let stdout_tx = output_tx.clone();
        let stdout_handle = tokio::spawn(async move {
            let mut index = 0;
            while let Some(result) = stdout_lines.next().await {
                match result {
                    Ok(line) => {
                        stdout_tx
                            .send(ProcessOutput::Stdout { index, line })
                            .await
                            .unwrap();
                        index += 1;
                    }
                    Err(e) => {
                        tracing::error!("failed to read stdout: {}", e);
                        break;
                    }
                }
            }
        });

        let stderr_tx = output_tx;
        let stderr_handle = tokio::spawn(async move {
            let mut index = 0;
            while let Some(result) = stderr_lines.next().await {
                match result {
                    Ok(line) => {
                        stderr_tx
                            .send(ProcessOutput::Stderr { index, line })
                            .await
                            .unwrap();
                        index += 1;
                    }
                    Err(e) => {
                        tracing::error!("failed to read stderr: {}", e);
                        break;
                    }
                }
            }
        });

        let _ = tokio::join!(stdout_handle, stderr_handle);
        let exit_status = child
            .wait()
            .await
            .map_err(|e| SandboxError(e.to_string()))?;

        Ok(exit_status)
    }
}
