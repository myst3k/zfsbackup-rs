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

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::{AmazonS3Builder, Checksum};
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{
    ClientOptions, GetOptions, GetRange, MultipartId, ObjectStore, PutPayload, RetryConfig,
};
use serde::{Deserialize, Serialize};
use crate::hash::Md5;
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
    #[error("etag mismatch for {key} part {part}: store returned {etag:?}, expected md5 {md5}")]
    EtagMismatch {
        key: String,
        part: u32,
        etag: String,
        md5: Md5,
    },
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
            StoreError::EtagMismatch { .. } => true,
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

    /// Misconfiguration or permission problems: retrying cannot help.
    pub fn is_terminal_config(&self) -> bool {
        match self {
            StoreError::Admin(admin::AdminError::S3 { code, .. }) => is_terminal_s3(code),
            StoreError::Backend(object_store::Error::Generic { source, .. })
            | StoreError::Backend(object_store::Error::NotFound { source, .. }) => {
                is_terminal_s3(&source.to_string())
            }
            StoreError::Config(_) => true,
            _ => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// Development only: accept any TLS certificate from the object store, so a
/// fault-injecting proxy can sit in front of a real endpoint. Never set in
/// production; every store logs a warning when it is on.
pub fn dev_allow_invalid_certs() -> bool {
    std::env::var("ZB_INSECURE_TLS").is_ok_and(|v| v == "1")
}

/// How a part upload is verified by the object store.
///
/// `Crc32c` (default) sends `x-amz-checksum-crc32c` so the store verifies
/// the body itself and refuses a mismatch; CRC32C is hardware accelerated
/// and Wasabi verifies it. `Md5` compares the returned ETag with a locally
/// computed MD5 (~0.6 GB/s per core) for an endpoint that does not verify
/// CRC32C on multipart uploads; `storage validate` reports which applies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PartChecksum {
    Md5,
    #[default]
    Crc32c,
}

impl std::str::FromStr for PartChecksum {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s {
            "md5" => Ok(Self::Md5),
            "crc32c" => Ok(Self::Crc32c),
            other => Err(format!("unknown part checksum {other:?} (md5 | crc32c)")),
        }
    }
}

impl PartChecksum {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Md5 => "md5",
            Self::Crc32c => "crc32c",
        }
    }
}

/// The digest computed for one part before upload, matching the store's
/// [`PartChecksum`].
#[derive(Clone, Copy, Debug)]
pub enum PartDigest {
    Md5(Md5),
    Crc32c(u32),
}

impl PartDigest {
    /// Compute the digest a store expects. CRC32C runs at memory speed and
    /// can be done inline; MD5 is slow enough that callers may prefer a
    /// blocking task.
    pub fn compute(kind: PartChecksum, data: &[u8]) -> Self {
        match kind {
            PartChecksum::Md5 => {
                use md5::Digest as _;
                Self::Md5(Md5(md5::Md5::digest(data).into()))
            }
            PartChecksum::Crc32c => {
                Self::Crc32c(crc_fast::checksum(crc_fast::CrcAlgorithm::Crc32Iscsi, data) as u32)
            }
        }
    }
}

/// One uploaded S3 part as recorded for CompleteMultipartUpload: the ETag
/// and, when the part was uploaded with a CRC32C, that checksum (S3 needs
/// it again on complete). Serialised as `etag` or `etag|<crc32c base64>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompletedPart {
    pub etag: String,
    pub crc32c: Option<u32>,
}

impl CompletedPart {
    pub fn parse(s: &str) -> Self {
        use base64::Engine as _;
        match s.split_once('|') {
            Some((etag, crc)) => {
                let crc = base64::engine::general_purpose::STANDARD
                    .decode(crc)
                    .ok()
                    .and_then(|b| <[u8; 4]>::try_from(b).ok())
                    .map(u32::from_be_bytes);
                Self {
                    etag: etag.to_string(),
                    crc32c: crc,
                }
            }
            None => Self {
                etag: s.to_string(),
                crc32c: None,
            },
        }
    }

    pub fn record(&self) -> String {
        match self.crc32c {
            Some(c) => format!("{}|{}", self.etag, crc32c_b64(c)),
            None => self.etag.clone(),
        }
    }
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
    /// How each multipart part is verified against the store.
    #[serde(default)]
    pub part_checksum: PartChecksum,
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
            .field("part_checksum", &self.part_checksum)
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

/// Where a store points; recorded in manifests so a moved bucket is detectable.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Location {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
}

/// Handle to one bucket.
#[derive(Clone)]
pub struct Store {
    inner: Arc<dyn BlobStore>,
    label: String,
    location: Option<Location>,
    /// Bucket-level calls `object_store` does not expose (ListMultipartUploads,
    /// checksummed UploadPart).
    admin: Option<Arc<admin::Admin>>,
    part_checksum: PartChecksum,
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
            part_checksum: cfg.part_checksum,
            label: format!("s3://{}@{}", cfg.bucket, cfg.endpoint),
            location: Some(Location {
                endpoint: cfg.endpoint.trim_end_matches('/').to_string(),
                region: cfg.region.clone(),
                bucket: cfg.bucket.clone(),
            }),
        })
    }

    /// In-memory store for tests, with S3-faithful multipart semantics
    /// (MD5 ETags per part, `NoSuchUpload` after complete/abort).
    pub fn memory() -> Self {
        Self {
            inner: Arc::new(fake::FakeS3::new()),
            label: "memory".into(),
            location: None,
            admin: None,
            part_checksum: PartChecksum::Md5,
        }
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn location(&self) -> Option<&Location> {
        self.location.as_ref()
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
    pub fn part_checksum(&self) -> PartChecksum {
        self.part_checksum
    }

    /// Upload one part, verified by `digest`: MD5 is compared with the
    /// returned ETag; CRC32C is sent with the request so the store verifies
    /// the body and refuses a mismatch. `part_index` is 0-based; S3 part
    /// numbers are 1-based.
    #[instrument(skip(self, data), fields(store = %self.label, len = data.len()))]
    pub async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_index: u32,
        data: Bytes,
        digest: PartDigest,
    ) -> Result<CompletedPart> {
        match (digest, &self.admin) {
            (PartDigest::Crc32c(crc), Some(admin)) => {
                let etag = admin
                    .upload_part_crc32c(key, upload_id, part_index + 1, data, crc)
                    .await
                    .map_err(|e| admin_error(e, key, upload_id))?;
                Ok(CompletedPart {
                    etag,
                    crc32c: Some(crc),
                })
            }
            (PartDigest::Crc32c(_), None) => {
                // Stores without a bucket API (tests) cannot carry checksums.
                let etag = self.put_part_raw(key, upload_id, part_index, data).await?;
                Ok(CompletedPart { etag, crc32c: None })
            }
            (PartDigest::Md5(md5), _) => {
                let etag = self.put_part_raw(key, upload_id, part_index, data).await?;
                if !etag_matches_md5(&etag, &md5) {
                    warn!(key, part_index, %etag, %md5, "etag mismatch");
                    return Err(StoreError::EtagMismatch {
                        key: key.into(),
                        part: part_index,
                        etag,
                        md5,
                    });
                }
                Ok(CompletedPart { etag, crc32c: None })
            }
        }
    }

    async fn put_part_raw(
        &self,
        key: &str,
        upload_id: &str,
        part_index: u32,
        data: Bytes,
    ) -> Result<String> {
        let id: MultipartId = upload_id.to_string();
        match self
            .inner
            .put_part(
                &Self::path(key),
                &id,
                part_index as usize,
                PutPayload::from_bytes(data),
            )
            .await
        {
            Ok(p) => Ok(p.content_id),
            Err(e) if is_no_such_upload(&e) => Err(StoreError::NoSuchUpload {
                key: key.into(),
                upload_id: upload_id.into(),
            }),
            Err(e) => Err(e.into()),
        }
    }

    /// Complete a multipart upload from the parts recorded, in order. Parts
    /// uploaded with a CRC32C are completed through the checksum-aware path
    /// (S3 requires the per-part checksums again on complete).
    #[instrument(skip(self, parts), fields(store = %self.label, parts = parts.len()))]
    pub async fn complete_multipart(
        &self,
        key: &str,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> Result<()> {
        if parts.iter().any(|p| p.crc32c.is_some()) {
            let admin = self.admin.as_ref().ok_or_else(|| {
                StoreError::Config("checksummed parts need a bucket API client".into())
            })?;
            return admin
                .complete_multipart_crc32c(key, upload_id, &parts)
                .await
                .map_err(|e| admin_error(e, key, upload_id));
        }
        let id: MultipartId = upload_id.to_string();
        let parts: Vec<PartId> = parts
            .into_iter()
            .map(|p| PartId { content_id: p.etag })
            .collect();
        match self
            .inner
            .complete_multipart(&Self::path(key), &id, parts)
            .await
        {
            Ok(_) => Ok(()),
            Err(e) if is_no_such_upload(&e) => Err(StoreError::NoSuchUpload {
                key: key.into(),
                upload_id: upload_id.into(),
            }),
            Err(e) => Err(e.into()),
        }
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

    /// Abort every incomplete multipart upload under `prefix`.
    pub async fn abort_uploads(&self, prefix: &str) -> Result<usize> {
        let mut n = 0;
        for (key, id) in self.list_multipart_uploads(prefix).await? {
            self.abort_multipart(&key, &id).await?;
            n += 1;
        }
        Ok(n)
    }

    /// Server-side copy of every object under `prefix` from `src` (same
    /// endpoint and credentials) into this store. Returns objects copied.
    pub async fn mirror_from(&self, src: &Store, prefix: &str) -> Result<usize> {
        let admin = self
            .admin
            .as_ref()
            .ok_or_else(|| StoreError::Config("mirror needs a bucket API client".into()))?;
        let src_bucket = src
            .location()
            .map(|l| l.bucket.clone())
            .ok_or_else(|| StoreError::Config("source store has no bucket".into()))?;
        let mut n = 0;
        for (key, _) in src.list(prefix).await? {
            admin
                .copy_from(&src_bucket, &key)
                .await
                .map_err(StoreError::Admin)?;
            n += 1;
        }
        Ok(n)
    }

    /// Abort every incomplete upload on `key` except `keep`. Called after a
    /// segment completes: a transport retry of CreateMultipartUpload whose
    /// first attempt succeeded but whose response was lost leaves an upload
    /// nobody knows the id of. Returns how many were aborted.
    pub async fn abort_stray_multipart(&self, key: &str, keep: &str) -> Result<usize> {
        let mut n = 0;
        for (k, id) in self.list_multipart_uploads(key).await? {
            if k == key && id != keep {
                self.abort_multipart(&k, &id).await?;
                n += 1;
            }
        }
        Ok(n)
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

    #[instrument(skip(self), fields(store = %self.label))]
    pub async fn get_range(&self, key: &str, range: Range<u64>) -> Result<Bytes> {
        let opts = GetOptions {
            range: Some(GetRange::Bounded(range)),
            ..Default::default()
        };
        let r = self
            .inner
            .get_opts(&Self::path(key), opts)
            .await
            .map_err(|e| map_nf(e, key))?;
        Ok(r.bytes().await?)
    }

    /// Object size, or `None` if absent.
    #[instrument(skip(self), fields(store = %self.label))]
    pub async fn head(&self, key: &str) -> Result<Option<u64>> {
        match self.inner.head(&Self::path(key)).await {
            Ok(m) => Ok(Some(m.size)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(e.into()),
        }
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

    /// Streaming listing for very large prefixes.
    pub fn list_stream(
        &self,
        prefix: &str,
    ) -> impl futures::Stream<Item = Result<(String, u64)>> + '_ {
        let p = Self::path(prefix);
        self.inner.list(Some(&p)).map(|r| {
            r.map(|m| (m.location.to_string(), m.size))
                .map_err(Into::into)
        })
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

/// S3 returns a part's ETag as the quoted hex MD5 of the part body (when the
/// bucket doesn't use SSE-KMS). Anything else is treated as a mismatch, which
/// is the safe direction: we re-upload rather than trust.
pub fn etag_matches_md5(etag: &str, md5: &Md5) -> bool {
    let t = etag.trim().trim_matches('"');
    t.len() == 32 && t.eq_ignore_ascii_case(&md5.to_hex())
}

/// In-memory backend that behaves like S3 where it matters to us.
pub mod fake {
    use std::collections::HashMap;
    use std::fmt;
    use std::sync::Mutex;

    use bytes::Bytes;
    use futures::stream::BoxStream;
    use md5::Digest as _;
    use object_store::memory::InMemory;
    use object_store::multipart::{MultipartStore, PartId};
    use object_store::path::Path;
    use object_store::{
        GetOptions, GetResult, ListResult, MultipartId, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    };

    #[derive(Default, Debug)]
    struct Upload {
        path: Path,
        parts: Vec<Option<Bytes>>,
    }

    #[derive(Debug)]
    pub struct FakeS3 {
        inner: InMemory,
        uploads: Mutex<HashMap<String, Upload>>,
        next: Mutex<u64>,
    }

    impl Default for FakeS3 {
        fn default() -> Self {
            Self::new()
        }
    }

    impl FakeS3 {
        pub fn new() -> Self {
            Self {
                inner: InMemory::new(),
                uploads: Mutex::new(HashMap::new()),
                next: Mutex::new(1),
            }
        }
    }

    impl fmt::Display for FakeS3 {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("FakeS3")
        }
    }

    fn no_such_upload(id: &str) -> object_store::Error {
        object_store::Error::Generic {
            store: "FakeS3",
            source: format!("NoSuchUpload: {id}").into(),
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for FakeS3 {
        async fn put_opts(&self, l: &Path, p: PutPayload, o: PutOptions) -> Result<PutResult> {
            self.inner.put_opts(l, p, o).await
        }
        async fn put_multipart_opts(
            &self,
            l: &Path,
            o: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(l, o).await
        }
        async fn get_opts(&self, l: &Path, o: GetOptions) -> Result<GetResult> {
            self.inner.get_opts(l, o).await
        }
        async fn delete(&self, l: &Path) -> Result<()> {
            self.inner.delete(l).await
        }
        fn list(&self, p: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(p)
        }
        async fn list_with_delimiter(&self, p: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(p).await
        }
        async fn copy(&self, a: &Path, b: &Path) -> Result<()> {
            self.inner.copy(a, b).await
        }
        async fn copy_if_not_exists(&self, a: &Path, b: &Path) -> Result<()> {
            self.inner.copy_if_not_exists(a, b).await
        }
    }

    #[async_trait::async_trait]
    impl MultipartStore for FakeS3 {
        async fn create_multipart(&self, path: &Path) -> Result<MultipartId> {
            let mut n = self.next.lock().unwrap();
            let id = format!("upload-{n}");
            *n += 1;
            self.uploads.lock().unwrap().insert(
                id.clone(),
                Upload {
                    path: path.clone(),
                    parts: Vec::new(),
                },
            );
            Ok(id)
        }

        async fn put_part(
            &self,
            _path: &Path,
            id: &MultipartId,
            part_idx: usize,
            data: PutPayload,
        ) -> Result<PartId> {
            let bytes = Bytes::from(data);
            let etag = format!("\"{}\"", hex::encode(md5::Md5::digest(&bytes)));
            let mut u = self.uploads.lock().unwrap();
            let up = u.get_mut(id).ok_or_else(|| no_such_upload(id))?;
            if up.parts.len() <= part_idx {
                up.parts.resize(part_idx + 1, None);
            }
            up.parts[part_idx] = Some(bytes);
            Ok(PartId { content_id: etag })
        }

        async fn complete_multipart(
            &self,
            path: &Path,
            id: &MultipartId,
            parts: Vec<PartId>,
        ) -> Result<PutResult> {
            let up = self
                .uploads
                .lock()
                .unwrap()
                .remove(id)
                .ok_or_else(|| no_such_upload(id))?;
            assert_eq!(&up.path, path);
            let mut buf = Vec::new();
            for (i, p) in parts.iter().enumerate() {
                let b = up.parts.get(i).and_then(|x| x.clone()).ok_or_else(|| {
                    object_store::Error::Generic {
                        store: "FakeS3",
                        source: format!("InvalidPart: missing part {i}").into(),
                    }
                })?;
                let etag = format!("\"{}\"", hex::encode(md5::Md5::digest(&b)));
                if etag != p.content_id {
                    return Err(object_store::Error::Generic {
                        store: "FakeS3",
                        source: format!("InvalidPart: etag mismatch for part {i}").into(),
                    });
                }
                buf.extend_from_slice(&b);
            }
            self.inner.put(path, PutPayload::from(buf)).await
        }

        async fn abort_multipart(&self, _path: &Path, id: &MultipartId) -> Result<()> {
            self.uploads
                .lock()
                .unwrap()
                .remove(id)
                .map(|_| ())
                .ok_or_else(|| no_such_upload(id))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use md5::Digest as _;

    fn md5_of(b: &[u8]) -> Md5 {
        Md5(md5::Md5::digest(b).into())
    }

    #[test]
    fn etag_compare() {
        let m = md5_of(b"abc");
        assert!(etag_matches_md5(&format!("\"{}\"", m.to_hex()), &m));
        assert!(etag_matches_md5(&m.to_hex().to_uppercase(), &m));
        assert!(!etag_matches_md5(
            "\"00000000000000000000000000000000\"",
            &m
        ));
        assert!(!etag_matches_md5("", &m));
    }

    #[tokio::test]
    async fn multipart_roundtrip_in_memory() {
        let s = Store::memory();
        let key = "v1/streams/x/seg-00000";
        let id = s.create_multipart(key).await.unwrap();
        let a = Bytes::from(vec![1u8; 10]);
        let b = Bytes::from(vec![2u8; 5]);
        let e0 = s
            .upload_part(key, &id, 0, a.clone(), PartDigest::Md5(md5_of(&a)))
            .await
            .unwrap();
        let e1 = s
            .upload_part(key, &id, 1, b.clone(), PartDigest::Md5(md5_of(&b)))
            .await
            .unwrap();
        s.complete_multipart(key, &id, vec![e0, e1]).await.unwrap();
        assert_eq!(s.head(key).await.unwrap(), Some(15));
        assert_eq!(
            s.get_range(key, 8..12).await.unwrap(),
            Bytes::from(vec![1, 1, 2, 2])
        );
        assert_eq!(s.get(key).await.unwrap().len(), 15);
        let keys = s.list("v1/streams/").await.unwrap();
        assert_eq!(keys.len(), 1);
        s.delete(key).await.unwrap();
        s.delete(key).await.unwrap(); // idempotent
        assert_eq!(s.head(key).await.unwrap(), None);
        assert!(matches!(s.get(key).await, Err(StoreError::NotFound(_))));
    }

    #[tokio::test]
    async fn abort_is_idempotent() {
        let s = Store::memory();
        let id = s.create_multipart("k").await.unwrap();
        s.abort_multipart("k", &id).await.unwrap();
        s.abort_multipart("k", &id).await.unwrap();
    }
}
