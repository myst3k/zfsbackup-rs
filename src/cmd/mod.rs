//! Command implementations.

pub mod list;
pub mod pin;
pub mod receive;
pub mod retention;
pub mod send;
pub mod verify;

use std::time::Duration;

use anyhow::{Context, bail};

use crate::manifest::{Manifest, keys};
use crate::store::{S3Config, Store};
use crate::types::Guid;

/// `s3://bucket` or `s3://bucket/prefix`.
pub struct Target {
    pub store: Store,
    pub prefix: String,
}

pub fn target(uri: &str, endpoint: Option<&str>, region: Option<&str>) -> anyhow::Result<Target> {
    let rest = uri
        .strip_prefix("s3://")
        .with_context(|| format!("{uri}: expected s3://bucket[/prefix]"))?;
    let (bucket, prefix) = match rest.split_once('/') {
        Some((b, p)) => (b, p.trim_matches('/')),
        None => (rest, ""),
    };
    if bucket.is_empty() {
        bail!("{uri}: empty bucket");
    }
    let endpoint = endpoint
        .map(str::to_string)
        .or_else(|| std::env::var("AWS_ENDPOINT_URL").ok())
        .context("no endpoint: pass --endpoint or set ZB_ENDPOINT / AWS_ENDPOINT_URL")?;
    let region = region
        .map(str::to_string)
        .or_else(|| std::env::var("AWS_REGION").ok())
        .context("no region: pass --region or set ZB_REGION / AWS_REGION")?;
    let access_key_id =
        std::env::var("AWS_ACCESS_KEY_ID").context("AWS_ACCESS_KEY_ID is not set")?;
    let secret_access_key =
        std::env::var("AWS_SECRET_ACCESS_KEY").context("AWS_SECRET_ACCESS_KEY is not set")?;
    let cfg = S3Config {
        endpoint,
        region,
        bucket: bucket.to_string(),
        access_key_id,
        secret_access_key,
        path_style: true,
        allow_http: false,
        sha256_checksums: false,
        part_checksum: Default::default(),
        max_retries: 10,
        retry_timeout_secs: 300,
        request_timeout_secs: 900,
    };
    Ok(Target {
        store: Store::s3(&cfg)?,
        prefix: prefix.to_string(),
    })
}

impl Target {
    /// Every manifest in the bucket, decoded. Undecodable manifests are
    /// reported and skipped — one corrupt object must not hide the rest.
    pub async fn manifests(&self) -> anyhow::Result<Vec<Manifest>> {
        let mut out = Vec::new();
        for (key, _) in self
            .store
            .list(&keys::all_manifests_prefix(&self.prefix))
            .await?
        {
            if !keys::is_manifest(&key) {
                continue;
            }
            let bytes = self.store.get(&key).await?;
            match Manifest::decode(&bytes) {
                Ok(m) => out.push(m),
                Err(e) => eprintln!("warning: {key}: undecodable manifest, skipping: {e}"),
            }
        }
        // Datasets together, then oldest first within each.
        out.sort_by(|a, b| {
            (a.dataset_guid, a.createtxg, a.snapshot_guid).cmp(&(
                b.dataset_guid,
                b.createtxg,
                b.snapshot_guid,
            ))
        });
        Ok(out)
    }

    pub async fn manifest_for(&self, snapshot: &str) -> anyhow::Result<Manifest> {
        let all = self.manifests().await?;
        all.into_iter()
            .find(|m| m.snapshot == snapshot)
            .with_context(|| format!("{snapshot} is not archived here (see `list`)"))
    }

    pub async fn pinned(&self) -> anyhow::Result<Vec<Guid>> {
        let mut out = Vec::new();
        for (key, _) in self.store.list(&keys::pins_prefix(&self.prefix)).await? {
            if let Some(g) = key.rsplit('/').next()
                && let Ok(g) = g.parse::<Guid>()
            {
                out.push(g);
            }
        }
        Ok(out)
    }
}

/// Retry a fallible upload/download step with capped exponential backoff.
pub async fn retry<T, F, Fut>(what: &str, mut op: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let mut attempt = 0u32;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) if attempt < 6 => {
                attempt += 1;
                let wait = Duration::from_millis(300u64.saturating_mul(1 << attempt.min(5)));
                tracing::warn!(what, attempt, error = %e, wait_ms = wait.as_millis() as u64, "retrying");
                tokio::time::sleep(wait).await;
            }
            Err(e) => return Err(e.context(format!("{what}: giving up after {attempt} retries"))),
        }
    }
}
