//! Object storage access for segments and manifests.
//!
//! Built on `object_store` (no AWS SDK). The important property we need from
//! the backend is the `MultipartStore` extension: an upload id plus explicit
//! part indices, so parts of one segment can be uploaded by different
//! gateways and completed later from the ETags recorded in the catalog.
//!
//! Every part upload is verified: the returned ETag must equal the MD5 we
//! computed while streaming the bytes. That is the gateway → object store
//! integrity check; BLAKE3 covers source → gateway.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::{AmazonS3Builder, Checksum};
use object_store::multipart::MultipartStore;
use object_store::path::Path;
use object_store::{ClientOptions, MultipartId, ObjectStore, PutPayload, RetryConfig};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, instrument, warn};

pub mod admin;
pub use object_store;

pub trait BlobStore: ObjectStore + MultipartStore {}
impl<T: ObjectStore + MultipartStore> BlobStore for T {}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("multipart upload {upload_id} for {key} no longer exists")]
    NoSuchUpload { key: String, upload_id: String },
    #[error("configuration: {0}")]
    Config(String),
    #[error("object store: {0}")]
    Backend(#[from] object_store::Error),
    #[error("s3: {0}")]
    Admin(#[from] admin::AdminError),
}

/// Map a bucket-API failure on a multipart call to the store's own shape.
fn admin_error(e: admin::AdminError, key: &str, upload_id: &str) -> StoreError {
    match &e {
        admin::AdminError::S3 { code, .. } if code == "NoSuchUpload" => StoreError::NoSuchUpload {
            key: key.into(),
            upload_id: upload_id.into(),
        },
        _ => StoreError::Admin(e),
    }
}

impl StoreError {
    /// True when retrying the same call later could succeed. `object_store`
    /// already retries transient HTTP failures internally; what surfaces
    /// here is either terminal or worth a slower outer retry.
    pub fn is_retryable(&self) -> bool {
        match self {
            StoreError::Backend(object_store::Error::Generic { source, .. }) => {
                !is_terminal_s3(&source.to_string())
            }
            StoreError::Backend(object_store::Error::NotFound { source, .. }) => {
                // A missing bucket surfaces as NotFound on multipart create.
                !is_terminal_s3(&source.to_string())
            }
            StoreError::Backend(_) => false,
            StoreError::Admin(admin::AdminError::Http(_)) => true,
            StoreError::Admin(admin::AdminError::S3 { status, code, .. }) => {
                *status >= 500
                    || matches!(
                        code.as_str(),
                        "SlowDown"
                            | "RequestTimeout"
                            | "InternalError"
                            | "ServiceUnavailable"
                            | "BadDigest"
                    )
            }
            _ => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// Development only: accept any TLS certificate from the object store, so a
/// fault-injecting proxy can sit in front of a real endpoint. Never set in
/// production; every store logs a warning when it is on.
pub fn dev_allow_invalid_certs() -> bool {
    crate::types::env_enabled("ZB_INSECURE_TLS")
}

/// S3's encoding of a CRC32C value: base64 of the big-endian 4 bytes.
pub fn crc32c_b64(c: u32) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(c.to_be_bytes())
}

/// S3-compatible endpoint configuration (Wasabi, MinIO, …).
#[derive(Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// e.g. `https://s3.us-east-2.wasabisys.com` or `http://minio:9000`
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// Path-style addressing (`endpoint/bucket/key`). Wasabi supports both;
    /// MinIO usually needs path style.
    #[serde(default = "default_true")]
    pub path_style: bool,
    /// Permit `http://` endpoints (local testing only).
    #[serde(default)]
    pub allow_http: bool,
    /// Send `x-amz-checksum-sha256` on uploads. Confirm the endpoint honours
    /// it before enabling in production.
    #[serde(default)]
    pub sha256_checksums: bool,
    #[serde(default = "default_retries")]
    pub max_retries: usize,
    #[serde(default = "default_retry_timeout_secs")]
    pub retry_timeout_secs: u64,
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"<redacted>")
            .field("path_style", &self.path_style)
            .field("allow_http", &self.allow_http)
            .finish_non_exhaustive()
    }
}

fn default_true() -> bool {
    true
}
fn default_retries() -> usize {
    20
}
fn default_retry_timeout_secs() -> u64 {
    30 * 60
}
fn default_request_timeout_secs() -> u64 {
    15 * 60
}

/// Handle to one bucket.
#[derive(Clone)]
pub struct Store {
    inner: Arc<dyn BlobStore>,
    label: String,
    /// Bucket-level calls `object_store` does not expose (ListMultipartUploads,
    /// checksummed UploadPart).
    admin: Option<Arc<admin::Admin>>,
    /// Cleared the first time the endpoint ignores `x-amz-checksum-crc32c`,
    /// so a store without that support is used plainly instead of failing
    /// every upload. Shared across clones: the decision is per endpoint.
    crc32c: Arc<AtomicBool>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store").field("label", &self.label).finish()
    }
}

impl Store {
    pub fn s3(cfg: &S3Config) -> Result<Self> {
        let retry = RetryConfig {
            max_retries: cfg.max_retries,
            retry_timeout: Duration::from_secs(cfg.retry_timeout_secs),
            ..RetryConfig::default()
        };
        let allow_invalid = dev_allow_invalid_certs();
        if allow_invalid {
            warn!(
                endpoint = cfg.endpoint,
                "ZB_INSECURE_TLS=1: TLS certificates are NOT verified (development only)"
            );
        }
        let client = ClientOptions::new()
            .with_allow_http(cfg.allow_http)
            .with_allow_invalid_certificates(allow_invalid)
            .with_timeout(Duration::from_secs(cfg.request_timeout_secs))
            .with_connect_timeout(Duration::from_secs(10))
            .with_pool_idle_timeout(Duration::from_secs(90))
            .with_pool_max_idle_per_host(256);
        // object_store's virtual-hosted mode expects the bucket in the
        // endpoint host itself; build that form so either style works.
        let endpoint = if cfg.path_style {
            cfg.endpoint.clone()
        } else {
            match cfg.endpoint.split_once("://") {
                Some((scheme, host)) => format!("{scheme}://{}.{host}", cfg.bucket),
                None => format!("{}.{}", cfg.bucket, cfg.endpoint),
            }
        };
        let mut b = AmazonS3Builder::new()
            .with_endpoint(&endpoint)
            .with_region(&cfg.region)
            .with_bucket_name(&cfg.bucket)
            .with_access_key_id(&cfg.access_key_id)
            .with_secret_access_key(&cfg.secret_access_key)
            .with_virtual_hosted_style_request(!cfg.path_style)
            .with_allow_http(cfg.allow_http)
            .with_retry(retry)
            .with_client_options(client);
        if cfg.sha256_checksums {
            b = b.with_checksum_algorithm(Checksum::SHA256);
        }
        let s3 = b.build().map_err(|e| StoreError::Config(e.to_string()))?;
        let admin = admin::Admin::new(cfg).map_err(|e| StoreError::Config(e.to_string()))?;
        Ok(Self {
            inner: Arc::new(s3),
            admin: Some(Arc::new(admin)),
            crc32c: Arc::new(AtomicBool::new(true)),
            label: format!("s3://{}@{}", cfg.bucket, cfg.endpoint),
        })
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    fn path(key: &str) -> Path {
        Path::from(key)
    }

    // ---- multipart -------------------------------------------------------

    #[instrument(skip(self), fields(store = %self.label))]
    pub async fn create_multipart(&self, key: &str) -> Result<String> {
        let id = self.inner.create_multipart(&Self::path(key)).await?;
        debug!(key, upload_id = %id, "multipart created");
        Ok(id)
    }

    /// How parts must be digested for this store.
    /// Check the endpoint and bucket: reachability, credentials, versioning,
    /// Object Lock, lifecycle, a read/write/delete probe, and whether the
    /// store really verifies the checksums uploads are sent with.
    ///
    /// `probe_prefix` scopes the temporary objects the probe writes.
    pub async fn validate(&self, probe_prefix: &str) -> Result<admin::Report> {
        let a = self.admin.as_ref().ok_or_else(|| {
            StoreError::Config("this store has no S3 admin client to check".into())
        })?;
        Ok(a.validate(Some(self), probe_prefix).await)
    }

    #[instrument(skip(self), fields(store = %self.label))]
    /// Incomplete multipart uploads whose key starts with `prefix`:
    /// (key, upload id). Empty for stores without a bucket API.
    pub async fn list_multipart_uploads(&self, prefix: &str) -> Result<Vec<(String, String)>> {
        match &self.admin {
            Some(a) => a
                .list_multipart_uploads(prefix)
                .await
                .map_err(|e| StoreError::Config(e.to_string())),
            None => Ok(Vec::new()),
        }
    }

    /// Every version and delete marker under `prefix`: (key, version id).
    pub async fn list_versions(&self, prefix: &str) -> Result<Vec<(String, String)>> {
        match &self.admin {
            Some(a) => a
                .list_object_versions(prefix)
                .await
                .map_err(StoreError::Admin),
            None => Ok(self
                .list(prefix)
                .await?
                .into_iter()
                .map(|(k, _)| (k, String::new()))
                .collect()),
        }
    }

    /// Remove everything under `prefix`: every object version, delete marker
    /// and incomplete multipart upload. Returns (versions, uploads) removed.
    /// Destructive — for development buckets and explicit operator use.
    pub async fn purge_prefix(&self, prefix: &str) -> Result<(usize, usize)> {
        let mut versions = 0;
        for (key, vid) in self.list_versions(prefix).await? {
            match &self.admin {
                Some(a) => a
                    .delete_version(&key, &vid)
                    .await
                    .map_err(StoreError::Admin)?,
                None => self.delete(&key).await?,
            }
            versions += 1;
        }
        let mut uploads = 0;
        for (key, id) in self.list_multipart_uploads(prefix).await? {
            self.abort_multipart(&key, &id).await?;
            uploads += 1;
        }
        Ok((versions, uploads))
    }

    pub async fn abort_multipart(&self, key: &str, upload_id: &str) -> Result<()> {
        let id: MultipartId = upload_id.to_string();
        match self.inner.abort_multipart(&Self::path(key), &id).await {
            Ok(()) => Ok(()),
            Err(e) if is_no_such_upload(&e) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    // ---- whole objects ---------------------------------------------------

    #[instrument(skip(self, data), fields(store = %self.label, len = data.len()))]
    /// PUT with `x-amz-checksum-crc32c` so the store verifies the body at
    /// write time and refuses a corrupt upload.
    ///
    /// Endpoints that ignore the header fall back to a plain PUT after the
    /// first attempt, with one warning: the archive is still protected by the
    /// BLAKE3 recorded per chunk, it is just checked on read rather than
    /// refused on write. `check` reports which kind of endpoint you have.
    pub async fn put_verified(&self, key: &str, data: Bytes) -> Result<()> {
        if let Some(a) = &self.admin
            && self.crc32c.load(Ordering::Relaxed)
        {
            let crc = crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32Iscsi, &data) as u32;
            match a.put_object_crc32c(key, data.clone(), crc).await {
                Ok(()) => return Ok(()),
                Err(admin::AdminError::S3 { code, .. }) if code == "ChecksumNotEchoed" => {
                    self.crc32c.store(false, Ordering::Relaxed);
                    warn!(
                        store = %self.label,
                        "endpoint does not verify CRC32C on upload; continuing with plain PUTs \
                         (chunks are still BLAKE3-checked by verify and receive)"
                    );
                }
                Err(e) => return Err(admin_error(e, key, "")),
            }
        }
        self.put(key, data).await
    }

    pub async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        self.inner
            .put(&Self::path(key), PutPayload::from_bytes(data))
            .await?;
        Ok(())
    }

    #[instrument(skip(self), fields(store = %self.label))]
    pub async fn get(&self, key: &str) -> Result<Bytes> {
        let r = self
            .inner
            .get(&Self::path(key))
            .await
            .map_err(|e| map_nf(e, key))?;
        Ok(r.bytes().await?)
    }

    /// Delete; a missing object is success (idempotent reclaim).
    #[instrument(skip(self), fields(store = %self.label))]
    pub async fn delete(&self, key: &str) -> Result<()> {
        match self.inner.delete(&Self::path(key)).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// All keys under a prefix (used by catalog rebuild and scrub).
    pub async fn list(&self, prefix: &str) -> Result<Vec<(String, u64)>> {
        let p = Self::path(prefix);
        let items: Vec<_> = self
            .inner
            .list(Some(&p))
            .map_ok(|m| (m.location.to_string(), m.size))
            .try_collect()
            .await?;
        Ok(items)
    }
}

fn is_terminal_s3(msg: &str) -> bool {
    [
        "NoSuchBucket",
        "AccessDenied",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "AuthorizationHeaderMalformed",
        "PermanentRedirect",
        "InvalidBucketName",
    ]
    .iter()
    .any(|c| msg.contains(c))
}

fn map_nf(e: object_store::Error, key: &str) -> StoreError {
    match e {
        object_store::Error::NotFound { .. } => StoreError::NotFound(key.to_string()),
        e => e.into(),
    }
}

fn is_no_such_upload(e: &object_store::Error) -> bool {
    let s = e.to_string();
    s.contains("NoSuchUpload") || s.contains("no such upload") || s.contains("UploadNotFound")
}
