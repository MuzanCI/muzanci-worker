use muzanci_config::config::ImageConfig;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

use muzanci_image::image::ImagePlatform;
use muzanci_image::image::ImagePlatformArchitecture;
use muzanci_image::image::ImagePlatformOs;
use muzanci_image::manifest_ref::ManifestRef;
use muzanci_image::reqwest_registry_client::ReqwestRegistryClient;
use muzanci_runner::sandbox::SandboxConfig;
use muzanci_runner::sandbox::Sandboxer;
use muzanci_runner::sandbox::jail_sandboxer::JailSandboxer;
use muzanci_runner::sandbox::zfs_image_store::ZfsImageStore;
use muzanci_runner::sandbox::zfs_image_store::ZfsPool;
use muzanci_transport::message::ProcessOutput;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let root_dir = PathBuf::from("/tmp/muzanci_runner/root_dir");
    std::fs::create_dir_all(&root_dir)?;

    eprintln!("Created test dir [{}]", root_dir.display());

    let bridge_if = "bridge0".to_string();

    let image_store = {
        let zfs_pool = ZfsPool::new("zroot");
        let registry_client = Arc::new(ReqwestRegistryClient::new());
        let image_store = ZfsImageStore::try_new(&root_dir, zfs_pool, registry_client)?;
        Arc::new(image_store)
    };

    let num_slots = 10;

    let sandboxer = JailSandboxer::try_new(&root_dir, bridge_if, image_store, num_slots)?;

    eprintln!("Created jail sandboxer");

    let sandbox_config = {
        let manifest_ref = ManifestRef::try_from("freebsd/freebsd-toolchain:15.0")?;
        let platform = ImagePlatform {
            architecture: ImagePlatformArchitecture::ARM64,
            os: ImagePlatformOs::FREEBSD,
        };
        let image = ImageConfig {
            manifest_ref,
            platform,
        };
        SandboxConfig {
            sandbox_id: Uuid::now_v7(),
            image,
        }
    };

    let sandbox = sandboxer.create(sandbox_config).await?;

    eprintln!("created sandbox");

    let cmd_strs = vec![
        "ping -c 3 1.1.1.1",
        "grep -E '^nameserver' /etc/resolv.conf",
    ];

    let envs = HashMap::new();
    let (output_tx, mut output_rx) = mpsc::channel(1);
    tokio::spawn(async move {
        while let Some(output) = output_rx.recv().await {
            let line = match &output {
                ProcessOutput::Stdout { line, .. } => line,
                ProcessOutput::Stderr { line, .. } => line,
            };
            eprintln!("{line}");
        }
    });
    for cmd_str in cmd_strs {
        let exit_status = sandbox.run(cmd_str, &envs, output_tx.clone()).await?;
        assert!(exit_status.success());
        eprintln!("successfully ran [{}]", cmd_str);
    }

    sandboxer.destroy(sandbox)?;

    eprintln!("destroyed sandbox");

    Ok(())
}
