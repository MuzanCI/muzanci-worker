use std::fs::File;
use std::io::Write;
use std::net::Ipv4Addr;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use crate::sandbox::zfs_image_store::ZfsImageStore;
use crate::sandbox::zfs_image_store::ZfsSnapshot;
use muzanci_image::image::ImagePlatform;
use muzanci_image::image::ImagePlatformOs;
use muzanci_image::manifest_ref::ManifestRef;

use crate::sandbox::NetworkInterface;
use crate::sandbox::Sandbox;
use crate::sandbox::SandboxConfig;
use crate::sandbox::SandboxId;
use crate::sandbox::Sandboxer;
use crate::sandbox::SandboxerError;
use crate::sandbox::jail_config::JailConfig;
use crate::sandbox::jail_sandbox::JailSandbox;
use crate::sandbox::jail_slot::FreeJailSlots;
use crate::sandbox::jail_slot::JailSlotId;

pub type ZfsDatasetQuotaGigabyte = usize;

pub enum JailRootfs {
    Linux {
        linux_dataset: String,
        linux_snapshot: ZfsSnapshot,
        freebsd_dataset: String,
        freebsd_snapshot: ZfsSnapshot,
    },
    FreeBSD {
        freebsd_dataset: String,
        freebsd_snapshot: ZfsSnapshot,
    },
}

#[derive(Clone)]
pub struct JailSandboxer {
    sandbox_dir: PathBuf,
    bridge_if: NetworkInterface,
    free_slots: FreeJailSlots,
    image_store: Arc<ZfsImageStore>,
}

impl JailSandboxer {
    pub fn try_new(
        root_dir: &Path,
        bridge_if: NetworkInterface,
        image_store: Arc<ZfsImageStore>,
        num_slots: usize,
    ) -> Result<Self, SandboxerError> {
        if !root_dir.is_dir() {
            let e = format!("root dir [{}] is not a directory", root_dir.display());
            return Err(SandboxerError(e));
        }

        let sandbox_dir = root_dir.join("sandbox.d");
        std::fs::create_dir_all(&root_dir).map_err(|e| SandboxerError(e.to_string()))?;

        // Assert bridge interface is ready to use.
        {
            let output = Command::new("sh")
                .arg("-c")
                .arg(format!("ifconfig {bridge_if}"))
                .output()
                .map_err(|e| SandboxerError(e.to_string()))?;
            if !output.status.success() {
                let e = String::from_utf8_lossy(&output.stderr).to_string();
                return Err(SandboxerError(e));
            }
        }

        let free_slots =
            FreeJailSlots::try_new(num_slots).map_err(|e| SandboxerError(e.to_string()))?;

        let jail_sandboxer = JailSandboxer {
            sandbox_dir,
            image_store,
            bridge_if,
            free_slots,
        };

        Ok(jail_sandboxer)
    }

    fn create_jail(
        &self,
        sandbox_config: &SandboxConfig,
        slot_id: JailSlotId,
        rootfs: JailRootfs,
        sandbox_dir: PathBuf,
        zfs_quota: ZfsDatasetQuotaGigabyte,
    ) -> Result<JailConfig, SandboxerError> {
        let jail_conf = self.jail_config(
            sandbox_config.sandbox_id.clone(),
            &sandbox_dir,
            slot_id,
            sandbox_config.image.platform.os.clone(),
            rootfs,
            zfs_quota,
        );
        let jail_conf_path = sandbox_dir.join("jail.conf");

        // Create jail configuration file.
        {
            let mut file =
                File::create(&jail_conf_path).map_err(|e| SandboxerError(e.to_string()))?;

            file.write_all(jail_conf.to_string().as_bytes())
                .map_err(|e| SandboxerError(e.to_string()))?;

            file.sync_all().map_err(|e| SandboxerError(e.to_string()))?;
        }

        // Create jail.
        {
            let output = Command::new("sh")
                .arg("-c")
                .arg(format!(
                    "jail -c -f {} {}",
                    jail_conf_path.display(),
                    jail_conf.name(),
                ))
                .output()
                .map_err(|e| SandboxerError(e.to_string()))?;
            if !output.status.success() {
                let e = String::from_utf8_lossy(&output.stderr).into_owned();
                Err(SandboxerError(e))?;
            }
        }

        // Apply resource limits to jail.
        {
            let cmd_strs = vec![
                format!("rctl -a jail:{}:pcpu:deny=100", jail_conf.name()),
                format!("rctl -a jail:{}:memoryuse:deny=1g", jail_conf.name()),
                format!("rctl -a jail:{}:maxproc:deny=512", jail_conf.name()),
                format!("rctl -a jail:{}:nthr:deny=512", jail_conf.name()),
                format!("rctl -a jail:{}:msgqqueued:deny=0", jail_conf.name()),
                format!("rctl -a jail:{}:msgqsize:deny=0", jail_conf.name()),
                format!("rctl -a jail:{}:nmsgq:deny=0", jail_conf.name()),
                format!("rctl -a jail:{}:nsem:deny=512", jail_conf.name()),
                format!("rctl -a jail:{}:nsemop:deny=32", jail_conf.name()),
                format!("rctl -a jail:{}:nshm:deny=32", jail_conf.name()),
                format!("rctl -a jail:{}:shmsize:deny=128M", jail_conf.name()),
            ];

            for cmd_str in cmd_strs {
                let output = Command::new("sh")
                    .arg("-c")
                    .arg(cmd_str)
                    .output()
                    .map_err(|e| SandboxerError(e.to_string()))?;
                if !output.status.success() {
                    let e = String::from_utf8_lossy(&output.stderr).into_owned();
                    Err(SandboxerError(e))?;
                }
            }
        }

        Ok(jail_conf)
    }

    fn jail_config(
        &self,
        sandbox_id: SandboxId,
        sandbox_dir: &Path,
        slot_id: JailSlotId,
        platform_os: ImagePlatformOs,
        rootfs: JailRootfs,
        zfs_quota: ZfsDatasetQuotaGigabyte,
    ) -> JailConfig {
        let sandbox_id = sandbox_id.to_string();
        let name = format!("sandbox-{sandbox_id}");
        let path = sandbox_dir.join("root");
        let hostname = format!("sandbox-{sandbox_id}.local");
        let epair_interface = format!("epair{slot_id}");
        let epair_host_interface = format!("epair{slot_id}a");
        let epair_jail_interface = format!("epair{slot_id}b");

        let ip_addr = Ipv4Addr::new(11, 0, 1, slot_id as u8);

        let exec_console_log = sandbox_dir.join("exec_console_log.txt");

        let exec_prepare = match &rootfs {
            JailRootfs::Linux {
                linux_dataset,
                linux_snapshot,
                freebsd_dataset,
                freebsd_snapshot,
            } => {
                vec![
                    format!(
                        "zfs clone -o mountpoint={} {freebsd_snapshot} {freebsd_dataset}",
                        path.display()
                    ),
                    format!("zfs set quota={zfs_quota}G {freebsd_dataset}"),
                    format!("mkdir -p {}/compat/linux", path.display()),
                    format!(
                        "zfs clone -o mountpoint={}/compat/linux {linux_snapshot} {linux_dataset}",
                        path.display()
                    ),
                ]
            }
            JailRootfs::FreeBSD {
                freebsd_dataset,
                freebsd_snapshot,
            } => {
                vec![
                    format!(
                        "zfs clone -o mountpoint={} {freebsd_snapshot} {freebsd_dataset}",
                        path.display()
                    ),
                    format!("zfs set quota={zfs_quota}G {freebsd_dataset}"),
                ]
            }
        };

        let exec_prestart = vec![
            // Create vmnet interface and peer
            format!("ifconfig {epair_interface} create up"),
            // Add vmnet interface peer to bridge
            format!(
                "ifconfig {} addm {} private {} up",
                self.bridge_if, epair_host_interface, epair_host_interface,
            ),
        ];

        let exec_created = vec![];

        let mut exec_start = vec![
            // Acquire IP address for vmnet interface
            format!(
                "/sbin/ifconfig {epair_jail_interface} inet {ip_addr} netmask 255.255.0.0 broadcast 11.0.255.255 up"
            ),
            // Add default route
            format!("/sbin/route add default 11.0.0.1"),
            // Add default nameserver
            format!("/bin/echo 'nameserver 1.1.1.1' > /etc/resolv.conf"),
            // Start base services
            format!("/bin/sh /etc/rc"),
        ];

        match &rootfs {
            JailRootfs::Linux { .. } => {
                exec_start.push(format!(
                    "/bin/echo 'nameserver 1.1.1.1' > /compat/linux/etc/resolv.conf"
                ));
            }
            JailRootfs::FreeBSD { .. } => {}
        }

        let exec_stop = vec![
            // Stop base services
            format!("/bin/sh /etc/rc.shutdown"),
        ];

        let exec_poststop = vec![
            // Remove vnet interface peer from bridge
            format!(
                "ifconfig {} deletem {}",
                self.bridge_if, epair_host_interface,
            ),
            // Destroy vnet interfaces.
            format!("ifconfig {} destroy", epair_host_interface),
        ];

        let exec_release = match &rootfs {
            JailRootfs::Linux {
                linux_dataset,
                freebsd_dataset,
                ..
            } => {
                vec![
                    format!("zfs destroy {}", linux_dataset),
                    format!("zfs destroy {}", freebsd_dataset),
                ]
            }
            JailRootfs::FreeBSD {
                freebsd_dataset, ..
            } => {
                vec![format!("zfs destroy {}", freebsd_dataset)]
            }
        };

        JailConfig::new(
            platform_os,
            name,
            slot_id,
            path,
            epair_jail_interface,
            hostname,
            exec_console_log,
            exec_prepare,
            exec_prestart,
            exec_created,
            exec_start,
            exec_stop,
            exec_poststop,
            exec_release,
        )
    }
}

#[async_trait::async_trait]
impl Sandboxer for JailSandboxer {
    async fn create(&self, config: SandboxConfig) -> Result<Arc<dyn Sandbox>, SandboxerError> {
        let slot = self
            .free_slots
            .reserve()
            .map_err(|e| SandboxerError(e.to_string()))?;

        // TODO: Validate config.platform.arch is supported.

        let rootfs = match config.image.platform.os {
            ImagePlatformOs::LINUX => {
                let linux_dataset = format!(
                    "{}/rootfs-linux-sandbox-{}",
                    self.image_store.zfs_pool(),
                    config.sandbox_id,
                );
                let linux_snapshot = self
                    .image_store
                    .snapshot(&config.image.manifest_ref, &config.image.platform)
                    .await
                    .map_err(|e| SandboxerError(e.to_string()))?;

                // Linux images must be run on a FreeBSD image.
                let freebsd_dataset = format!(
                    "{}/rootfs-freebsd-sandbox-{}",
                    self.image_store.zfs_pool(),
                    config.sandbox_id,
                );
                let freebsd_snapshot = {
                    let freebsd_manifest_ref =
                        ManifestRef::try_from("freebsd/freebsd-toolchain:15.0")
                            .map_err(|e| SandboxerError(e.to_string()))?;
                    let freebsd_platform = ImagePlatform {
                        os: ImagePlatformOs::FREEBSD,
                        architecture: config.image.platform.architecture.clone(),
                    };
                    self.image_store
                        .snapshot(&freebsd_manifest_ref, &freebsd_platform)
                        .await
                        .map_err(|e| SandboxerError(e.to_string()))?
                };

                JailRootfs::Linux {
                    linux_dataset,
                    linux_snapshot,
                    freebsd_dataset,
                    freebsd_snapshot,
                }
            }
            ImagePlatformOs::FREEBSD => {
                let freebsd_dataset = format!(
                    "{}/rootfs-freebsd-sandbox-{}",
                    self.image_store.zfs_pool(),
                    config.sandbox_id,
                );
                let freebsd_snapshot = {
                    let freebsd_manifest_ref =
                        ManifestRef::try_from("freebsd/freebsd-toolchain:15.0")
                            .map_err(|e| SandboxerError(e.to_string()))?;
                    let freebsd_platform = ImagePlatform {
                        os: ImagePlatformOs::FREEBSD,
                        architecture: config.image.platform.architecture.clone(),
                    };
                    self.image_store
                        .snapshot(&freebsd_manifest_ref, &freebsd_platform)
                        .await
                        .map_err(|e| SandboxerError(e.to_string()))?
                };

                JailRootfs::FreeBSD {
                    freebsd_dataset,
                    freebsd_snapshot,
                }
            }
            ImagePlatformOs::OTHER(_) => {
                return Err(SandboxerError(format!(
                    "Unsupported platform: [{}]",
                    config.image.platform.os
                )));
            }
        };

        let sandbox_dir = self.sandbox_dir.join(&config.sandbox_id.to_string());
        std::fs::create_dir_all(&sandbox_dir).map_err(|e| SandboxerError(e.to_string()))?;

        let zfs_quota = 10;

        let jail_conf =
            self.create_jail(&config, slot.slot_id(), rootfs, sandbox_dir, zfs_quota)?;

        let sandbox = JailSandbox::try_new(config, jail_conf, slot)
            .map_err(|e| SandboxerError(e.to_string()))?;

        Ok(Arc::new(sandbox))
    }

    fn destroy(&self, sandbox: Arc<dyn Sandbox>) -> Result<(), SandboxerError> {
        let sandbox_dir = self
            .sandbox_dir
            .join(&sandbox.config().sandbox_id.to_string());

        let jail_conf_path = sandbox_dir.join("jail.conf");
        let jail_name = format!("sandbox-{}", &sandbox.config().sandbox_id.to_string());

        // Destroy jail.
        {
            let output = Command::new("sh")
                .arg("-c")
                .arg(format!(
                    "jail -r -f {} {}",
                    jail_conf_path.display(),
                    jail_name,
                ))
                .output()
                .map_err(|e| SandboxerError(e.to_string()))?;
            if !output.status.success() {
                let e = String::from_utf8_lossy(&output.stderr).into_owned();
                return Err(SandboxerError(e));
            }
        }

        // Remove resource limits.
        {
            let output = Command::new("sh")
                .arg("-c")
                .arg(format!("rctl -r jail:{}", jail_name))
                .output()
                .map_err(|e| SandboxerError(e.to_string()))?;
            if !output.status.success() {
                let e = String::from_utf8_lossy(&output.stderr).into_owned();
                return Err(SandboxerError(e));
            }
        }

        Ok(())
    }
}
