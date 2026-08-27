use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::process::Output;
use std::sync::Arc;

use async_compression::tokio::bufread::GzipDecoder;
use async_fd_lock::LockRead;
use async_fd_lock::LockWrite;
use async_fd_lock::RwLockReadGuard;
use async_fd_lock::RwLockWriteGuard;
use sha2::Digest as _;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tokio_tar::Archive;

use muzanci_image::blob_ref::BlobRef;
use muzanci_image::digest::Digest;
use muzanci_image::image::Descriptor;
use muzanci_image::image::ImageConfig;
use muzanci_image::image::ImageManifest;
use muzanci_image::image::ImagePlatform;
use muzanci_image::image::MediaType;
use muzanci_image::manifest_ref::ManifestRef;
use muzanci_image::registry_client::RegistryClient;

#[derive(Debug, Clone)]
pub struct ZfsDataset {
    pool: ZfsPool,
    name: String,
    mountpoint: PathBuf,
}

impl std::fmt::Display for ZfsDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.pool, self.name)
    }
}

/// A ZFS snapshot.
#[derive(Clone)]
pub struct ZfsSnapshot {
    pool: ZfsPool,
    dataset_name: String,
}

impl std::fmt::Display for ZfsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}@final", self.pool, self.dataset_name)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ZfsPoolError(String);

#[derive(Debug, Clone)]
pub struct ZfsPool(String);

impl ZfsPool {
    /// Constructs a new `ZfsPool` with the given name.
    /// This does **NOT** guarantee that the pool exists on the system.
    pub fn new(name: &str) -> Self {
        Self(name.to_string())
    }

    /// Checks if the pool exists on the system.
    pub fn exists(&self) -> Result<bool, ZfsPoolError> {
        {
            let cmd_str = format!("zpool list {self}");
            let output = self.run_cmd(&cmd_str)?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
                let not_exist = format!("cannot open '{self}': pool does not exist");
                if stderr.contains(&not_exist) {
                    return Ok(false);
                }
                return Err(ZfsPoolError(stderr));
            }
        }

        Ok(true)
    }

    /// Creates a new dataset with the given name and mounts it at the mountpoint.
    pub async fn dataset_create(
        &self,
        name: &str,
        mountpoint: &Path,
    ) -> Result<ZfsDataset, ZfsPoolError> {
        let dataset = ZfsDataset {
            pool: self.clone(),
            name: name.to_string(),
            mountpoint: mountpoint.to_path_buf(),
        };

        {
            let mountpoint = mountpoint.to_string_lossy();
            let cmd_str = format!("zfs create -o mountpoint={mountpoint} {dataset}");
            let output = self.run_cmd(&cmd_str)?;
            if !output.status.success() {
                return Err(ZfsPoolError(
                    String::from_utf8_lossy(&output.stderr).into_owned(),
                ));
            }
        }

        Ok(dataset)
    }

    /// Clones a snapshot to create a new dataset with the given name and mounts it at the mountpoint.
    pub async fn snapshot_clone(
        &self,
        snapshot: &ZfsSnapshot,
        name: &str,
        mountpoint: &Path,
    ) -> Result<ZfsDataset, ZfsPoolError> {
        let dataset = ZfsDataset {
            pool: self.clone(),
            name: name.to_string(),
            mountpoint: mountpoint.to_path_buf(),
        };

        {
            let mountpoint = mountpoint.to_string_lossy();
            let cmd_str = format!("zfs clone -o mountpoint={mountpoint} {snapshot} {dataset}");
            let output = self.run_cmd(&cmd_str)?;
            if !output.status.success() {
                return Err(ZfsPoolError(
                    String::from_utf8_lossy(&output.stderr).into_owned(),
                ));
            }
        }

        Ok(dataset)
    }

    /// Checks if a snapshot exists or not.
    pub async fn snapshot_exists(&self, snapshot: &ZfsSnapshot) -> Result<bool, ZfsPoolError> {
        {
            let cmd_str = format!("zfs list -t snapshot {snapshot}");
            let output = self.run_cmd(&cmd_str)?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let not_exist = format!("cannot open '{snapshot}': dataset does not exist");
                if stderr.contains(&not_exist) {
                    return Ok(false);
                } else {
                    return Err(ZfsPoolError(stderr.into_owned()));
                }
            }
        }

        Ok(true)
    }

    /// Snapshots a dataset to make it immutable and clonable.
    pub async fn snapshot_create(&self, dataset: &ZfsDataset) -> Result<ZfsSnapshot, ZfsPoolError> {
        {
            let cmd_str = format!("zfs set readonly=on {dataset}");
            let output = self.run_cmd(&cmd_str)?;
            if !output.status.success() {
                return Err(ZfsPoolError(
                    String::from_utf8_lossy(&output.stderr).into_owned(),
                ));
            }
        }

        let snapshot = ZfsSnapshot {
            pool: dataset.pool.clone(),
            dataset_name: dataset.name.clone(),
        };

        {
            let cmd_str = format!("zfs snapshot {snapshot}");
            let output = self.run_cmd(&cmd_str)?;
            if !output.status.success() {
                return Err(ZfsPoolError(
                    String::from_utf8_lossy(&output.stderr).into_owned(),
                ));
            }
        }

        Ok(snapshot)
    }

    /// Destroys a dataset and any associated snapshots.
    pub async fn destroy(&self, dataset: ZfsDataset) -> Result<(), ZfsPoolError> {
        {
            let cmd_str = format!("zfs destroy {dataset}");
            let output = self.run_cmd(&cmd_str)?;
            if !output.status.success() {
                return Err(ZfsPoolError(
                    String::from_utf8_lossy(&output.stderr).into_owned(),
                ));
            }
        }
        Ok(())
    }

    fn run_cmd(&self, cmd_str: &str) -> Result<Output, ZfsPoolError> {
        let mut cmd = std::process::Command::new("sh");
        cmd.arg("-c").arg(cmd_str);
        cmd.output().map_err(|e| ZfsPoolError(e.to_string()))
    }
}

impl std::fmt::Display for ZfsPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ZfsImageStoreError(String);

#[derive(Clone)]
pub struct ZfsImageStore {
    gc_lock_path: PathBuf,
    ingest_dir: PathBuf,
    blob_dir: PathBuf,
    chain_lock_dir: PathBuf,
    chain_dir: PathBuf,
    zfs_pool: ZfsPool,
    registry_client: Arc<dyn RegistryClient>,
}

impl ZfsImageStore {
    pub fn try_new(
        root_dir: &Path,
        zfs_pool: ZfsPool,
        registry_client: Arc<dyn RegistryClient>,
    ) -> Result<Self, ZfsImageStoreError> {
        if !root_dir.is_dir() {
            let e = format!("root dir [{}] is not a directory", root_dir.display());
            return Err(ZfsImageStoreError(e));
        }

        if !zfs_pool
            .exists()
            .map_err(|e| ZfsImageStoreError(e.to_string()))?
        {
            let e = format!("zfs pool [{zfs_pool}] does not exist");
            return Err(ZfsImageStoreError(e));
        }

        let ingest_dir = root_dir.join("ingest.d");
        std::fs::create_dir_all(&ingest_dir).map_err(|e| ZfsImageStoreError(e.to_string()))?;
        let blob_dir = root_dir.join("blob.d");
        std::fs::create_dir_all(&blob_dir).map_err(|e| ZfsImageStoreError(e.to_string()))?;
        let chain_lock_dir = root_dir.join("chain.lock.d");
        std::fs::create_dir_all(&chain_lock_dir).map_err(|e| ZfsImageStoreError(e.to_string()))?;
        let chain_dir = root_dir.join("chain.d");
        std::fs::create_dir_all(&chain_dir).map_err(|e| ZfsImageStoreError(e.to_string()))?;

        Ok(Self {
            gc_lock_path: root_dir.join("gc.lock"),
            ingest_dir,
            blob_dir,
            chain_lock_dir,
            chain_dir,
            registry_client,
            zfs_pool,
        })
    }

    pub fn zfs_pool(&self) -> &ZfsPool {
        &self.zfs_pool
    }

    #[tracing::instrument(skip_all)]
    pub async fn snapshot(
        &self,
        manifest_ref: &ManifestRef,
        platform: &ImagePlatform,
    ) -> Result<ZfsSnapshot, ZfsImageStoreError> {
        let image_manifest = self
            .registry_client
            .resolve_image_manifest(manifest_ref, platform)
            .await
            .map_err(|e| ZfsImageStoreError(e.to_string()))?;

        let _gc_read_guard = self.gc_read_lock().await?;

        // Download all blob digests concurrently.
        {
            let blob_refs = {
                let mut blob_refs = image_manifest
                    .layers
                    .iter()
                    .map(|l| BlobRef {
                        domain: manifest_ref.domain.clone(),
                        namespace: manifest_ref.namespace.clone(),
                        digest: l.digest.clone(),
                        size: l.size,
                        media_type: l.media_type.clone(),
                    })
                    .collect::<Vec<_>>();

                blob_refs.push(BlobRef {
                    domain: manifest_ref.domain.clone(),
                    namespace: manifest_ref.namespace.clone(),
                    digest: image_manifest.config.digest.clone(),
                    size: image_manifest.config.size,
                    media_type: image_manifest.config.media_type.clone(),
                });
                blob_refs
            };

            let mut join_set = JoinSet::new();

            for blob_ref in blob_refs {
                let image_store = self.clone();
                join_set.spawn(async move {
                    match image_store.get_local_blob(&blob_ref.digest).await? {
                        None => image_store.pull_remote_blob(blob_ref).await,
                        Some(_) => Ok(()),
                    }
                });
            }

            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => return Err(e),
                    Err(e) => return Err(ZfsImageStoreError(e.to_string())),
                }
            }
        }

        let snapshot = self.build_image_snapshot(&image_manifest).await?;

        Ok(snapshot)
    }

    #[tracing::instrument(skip_all)]
    pub async fn collect_garbage(&self) -> Result<(), ZfsImageStoreError> {
        let _ = self.gc_write_lock().await;
        unimplemented!();
    }

    /// Acquire read lock on garbage collector lock file.
    #[tracing::instrument(skip_all)]
    async fn gc_read_lock(&self) -> Result<RwLockReadGuard<tokio::fs::File>, ZfsImageStoreError> {
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.gc_lock_path)
            .await
            .map_err(|e| ZfsImageStoreError(e.to_string()))?;
        let read_guard = file
            .lock_read()
            .await
            .map_err(|_| ZfsImageStoreError("gc lock read failed".to_string()))?;
        Ok(read_guard)
    }

    /// Acquire write lock on garbage collector lock file.
    #[tracing::instrument(skip_all)]
    async fn gc_write_lock(&self) -> Result<RwLockWriteGuard<tokio::fs::File>, ZfsImageStoreError> {
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.gc_lock_path)
            .await
            .map_err(|e| ZfsImageStoreError(e.to_string()))?;
        let write_guard = file
            .lock_write()
            .await
            .map_err(|_| ZfsImageStoreError("gc lock write failed".to_string()))?;
        Ok(write_guard)
    }

    /// Acquire write lock on ingest lock file.
    #[tracing::instrument(skip_all)]
    async fn ingest_write_lock(
        &self,
        blob_ref: &BlobRef,
    ) -> Result<RwLockWriteGuard<tokio::fs::File>, ZfsImageStoreError> {
        let path = self.ingest_dir.join(blob_ref.digest.to_string());
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await
            .map_err(|e| ZfsImageStoreError(e.to_string()))?;
        let write_guard = file
            .lock_write()
            .await
            .map_err(|_| ZfsImageStoreError("ingest lock write failed".to_string()))?;
        Ok(write_guard)
    }

    /// Acquire write lock on chain lock file.
    #[tracing::instrument(skip_all)]
    async fn chain_write_lock(
        &self,
        chain_id: &Digest,
    ) -> Result<RwLockWriteGuard<tokio::fs::File>, ZfsImageStoreError> {
        let path = self.chain_lock_dir.join(chain_id.to_string());
        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await
            .map_err(|e| ZfsImageStoreError(e.to_string()))?;
        let write_guard = file
            .lock_write()
            .await
            .map_err(|_| ZfsImageStoreError("chain lock write failed".to_string()))?;
        Ok(write_guard)
    }

    /// Download a blob from a registry.
    #[tracing::instrument(skip_all)]
    async fn pull_remote_blob(&self, blob_ref: BlobRef) -> Result<(), ZfsImageStoreError> {
        let mut write_guard = self.ingest_write_lock(&blob_ref).await?;

        let mut hasher = sha2::Sha256::new();
        let mut bytes_written = 0;
        let mut buf = [0u8; 16 * 1024];

        // Read any already ingested data from the file and update the hasher and bytes_written.
        {
            let mut file_reader = tokio::io::BufReader::new(&mut write_guard);

            loop {
                let n = file_reader
                    .read(&mut buf)
                    .await
                    .map_err(|e| ZfsImageStoreError(e.to_string()))?;
                if n == 0 {
                    break; // EOF
                }
                hasher.update(&buf[..n]);
                bytes_written += n;
            }
        }

        tracing::info!(
            "Already ingested {} bytes for digest {}",
            bytes_written,
            blob_ref.digest
        );

        // If the blob is not fully ingested, read the remaining data from the registry and write to the file, while updating the hasher and bytes_written.
        if bytes_written < blob_ref.size {
            tracing::info!(
                "Ingesting remaining {} bytes for digest {} from registry",
                blob_ref.size - bytes_written,
                blob_ref.digest
            );
            let mut reader = self
                .registry_client
                .blob_reader(&blob_ref, bytes_written)
                .await
                .map_err(|e| ZfsImageStoreError(e.to_string()))?;

            loop {
                // Read from registry at offset and write to file until EOF.
                let n = reader
                    .read(&mut buf)
                    .await
                    .map_err(|e| ZfsImageStoreError(e.to_string()))?;
                if n == 0 {
                    break; // EOF
                }
                write_guard
                    .write_all(&buf[..n])
                    .await
                    .map_err(|e| ZfsImageStoreError(e.to_string()))?;
                hasher.update(&buf[..n]);
                bytes_written += n;
            }
            write_guard
                .inner()
                .sync_all()
                .await
                .map_err(|e| ZfsImageStoreError(e.to_string()))?;
        }

        tracing::info!(
            "Finished ingesting {} bytes for digest {}",
            bytes_written,
            blob_ref.digest
        );

        // Validate the blob size.
        if bytes_written != blob_ref.size {
            let e = format!(
                "Blob size mismatch for digest {}: expected {} bytes, got {} bytes",
                blob_ref.digest, blob_ref.size, bytes_written
            );

            tracing::error!("{e}");

            return Err(ZfsImageStoreError(e));
        }

        // Validate the blob digest.
        let local_digest = Digest::from(hasher);

        if local_digest != blob_ref.digest {
            let e = format!(
                "Blob digest mismatch for digest {}: expected {}, got {}",
                blob_ref.digest, blob_ref.digest, local_digest
            );

            tracing::error!("{e}");

            return Err(ZfsImageStoreError(e));
        }

        tracing::info!(
            "Atomically renaming ingest file to content store for blob: {}",
            blob_ref.digest,
        );

        // Atomically rename the file from staging area to content store.
        let blob_path = self.blob_dir.join(blob_ref.digest.to_string());
        let ingest_path = self.ingest_dir.join(blob_ref.digest.to_string());
        tokio::fs::rename(&ingest_path, &blob_path)
            .await
            .map_err(|e| ZfsImageStoreError(e.to_string()))?;

        Ok(())
    }

    /// Get a local blob file by digest.
    #[tracing::instrument(skip_all)]
    async fn get_local_blob(
        &self,
        digest: &Digest,
    ) -> Result<Option<tokio::fs::File>, ZfsImageStoreError> {
        let path = self.blob_dir.join(digest.to_string());
        if !path.exists() {
            return Ok(None);
        }

        match tokio::fs::File::open(&path)
            .await
            .map_err(|e| ZfsImageStoreError(e.to_string()))
        {
            Ok(file) => Ok(Some(file)),
            Err(e) => Err(ZfsImageStoreError(e.to_string())),
        }
    }

    /// Build an image snapshot from the image manifest.
    #[tracing::instrument(skip_all)]
    async fn build_image_snapshot(
        &self,
        image_manifest: &ImageManifest,
    ) -> Result<ZfsSnapshot, ZfsImageStoreError> {
        // Get diff IDs and layer descriptors.
        let (diff_ids, layers) = {
            let image_config = {
                let mut image_config_file = match self
                    .get_local_blob(&image_manifest.config.digest)
                    .await?
                {
                    Some(file) => file,
                    None => return Err(ZfsImageStoreError("image config not found".to_string())),
                };
                let mut bytes = Vec::new();
                image_config_file
                    .read_to_end(&mut bytes)
                    .await
                    .map_err(|e| ZfsImageStoreError(e.to_string()))?;
                serde_json::from_slice::<ImageConfig>(&bytes)
                    .map_err(|e| ZfsImageStoreError(e.to_string()))?
            };

            let diff_ids = image_config.rootfs.diff_ids;

            let layers = &image_manifest.layers;

            if diff_ids.len() != layers.len() {
                return Err(ZfsImageStoreError(
                    "diff_ids and layers have different lengths".to_string(),
                ));
            }

            (diff_ids, layers)
        };

        // Iteratively build the chain.
        let mut parent_snapshot: Option<ZfsSnapshot> = None;

        for (diff_id, layer) in diff_ids.into_iter().zip(layers.into_iter()) {
            let chain_id = match &parent_snapshot {
                None => diff_id,
                Some(parent_snapshot) => {
                    // Compute chain ID from current and parent.
                    let parent_chain_id = parent_snapshot.dataset_name.clone();
                    let preimage = format!("{} {}", parent_chain_id, diff_id);
                    let mut hasher = sha2::Sha256::new();
                    hasher.update(preimage.as_bytes());
                    Digest::Sha256(hex::encode(hasher.finalize()))
                }
            };

            let _chain_guard = self.chain_write_lock(&chain_id).await?;

            let current_snapshot = ZfsSnapshot {
                pool: self.zfs_pool.clone(),
                dataset_name: chain_id.to_string(),
            };

            if !self
                .zfs_pool
                .snapshot_exists(&current_snapshot)
                .await
                .map_err(|e| ZfsImageStoreError(e.to_string()))?
            {
                self.build_chain_snapshot(&chain_id, layer, parent_snapshot)
                    .await?;
            }

            parent_snapshot = Some(current_snapshot);
        }

        match parent_snapshot {
            Some(snapshot) => Ok(snapshot),
            None => Err(ZfsImageStoreError("diff_ids is empty".to_string())),
        }
    }

    /// Build a snapshot from a layer and parent snapshot.
    #[tracing::instrument(skip_all)]
    async fn build_chain_snapshot(
        &self,
        chain_id: &Digest,
        layer: &Descriptor,
        parent_snapshot: Option<ZfsSnapshot>,
    ) -> Result<ZfsSnapshot, ZfsImageStoreError> {
        let layer_blob_file = match self.get_local_blob(&layer.digest).await? {
            Some(file) => file,
            None => return Err(ZfsImageStoreError("layer blob not found".to_string())),
        };

        let diff = match layer.media_type {
            MediaType::OciImageLayerV1TarGzip => {
                let reader = BufReader::new(layer_blob_file);
                let decoder = GzipDecoder::new(reader);
                Archive::new(decoder)
            }
            _ => {
                return Err(ZfsImageStoreError(
                    "unsupported layer media type".to_string(),
                ));
            }
        };

        let chain_dir = self.chain_dir.join(&chain_id.to_string());

        let working_dataset = match parent_snapshot {
            Some(parent_snapshot) => self
                .zfs_pool
                .snapshot_clone(&parent_snapshot, &chain_id.to_string(), &chain_dir)
                .await
                .map_err(|e| ZfsImageStoreError(e.to_string()))?,
            None => self
                .zfs_pool
                .dataset_create(&chain_id.to_string(), &chain_dir)
                .await
                .map_err(|e| ZfsImageStoreError(e.to_string()))?,
        };

        self.apply_diff(&working_dataset, diff).await?;

        self.zfs_pool
            .snapshot_create(&working_dataset)
            .await
            .map_err(|e| ZfsImageStoreError(e.to_string()))
    }

    #[tracing::instrument(skip_all)]
    async fn apply_diff(
        &self,
        dataset: &ZfsDataset,
        mut diff: Archive<GzipDecoder<BufReader<tokio::fs::File>>>,
    ) -> Result<(), ZfsImageStoreError> {
        let mut entries = diff
            .entries()
            .map_err(|e| ZfsImageStoreError(e.to_string()))?;

        while let Some(entry_result) = entries.next().await {
            let mut entry = entry_result.map_err(|e| ZfsImageStoreError(e.to_string()))?;

            let raw_entry_path = entry
                .path()
                .map_err(|e| ZfsImageStoreError(e.to_string()))?;

            let entry_path = sanitize_and_validate_path(&raw_entry_path)
                .map_err(|e| ZfsImageStoreError(e.to_string()))?;

            if entry_path.as_os_str().is_empty() {
                // Skip root directory entries
                continue;
            }

            let file_name = match entry_path.file_name().and_then(|s| s.to_str()) {
                Some(name) => name,
                // Skip file names ending with ".."
                None => continue,
            };

            if file_name == ".wh..wh..opq" {
                // Remove all contents of parent directory.
                if let Some(entry_parent_dir) = entry_path.parent() {
                    std::fs::remove_dir_all(&entry_parent_dir)
                        .map_err(|e| ZfsImageStoreError(e.to_string()))?;
                }
                continue;
            }

            if let Some(whiteout_file_name) = file_name.strip_prefix(".wh.") {
                if let Some(entry_parent_dir) = entry_path.parent() {
                    let whiteout_path = entry_parent_dir.join(whiteout_file_name);
                    std::fs::remove_file(&whiteout_path)
                        .map_err(|e| ZfsImageStoreError(e.to_string()))?;
                }
                continue;
            }

            entry
                .unpack_in(&dataset.mountpoint)
                .await
                .map_err(|e| ZfsImageStoreError(e.to_string()))?;
        }

        Ok(())
    }
}

/// Prevents Path Traversal ("Zip Slip") by ensuring no components jump outside the root.
fn sanitize_and_validate_path(raw_path: &Path) -> Result<PathBuf, std::io::Error> {
    let mut clean_path = PathBuf::new();

    for component in raw_path.components() {
        match component {
            Component::Normal(c) => clean_path.push(c),
            Component::CurDir => continue, // Ignore "."
            Component::ParentDir => {
                // Return an error if attempt to traverse higher than relative root
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("Path traversal attempt detected in entry: {:?}", raw_path),
                ));
            }
            Component::Prefix(_) | Component::RootDir => continue, // Strip absolute roots
        }
    }

    Ok(clean_path)
}
