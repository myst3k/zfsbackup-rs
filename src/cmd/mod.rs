//! Command implementations.

pub mod check;
pub mod clean;
pub mod list;
pub mod pin;
pub mod receive;
pub mod retention;
pub mod send;
pub mod verify;

use std::time::Duration;

use anyhow::{Context, bail};
use futures::stream::{StreamExt, TryStreamExt};

use crate::manifest::{Manifest, keys};
use crate::store::{S3Config, Store};
use crate::types::Guid;

/// How to reach the store, shared by every command.
#[derive(Clone, Debug, Default)]
pub struct Conn {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    /// Permit a plain-HTTP endpoint (a MinIO or Ceph on a trusted network).
    pub allow_http: bool,
    /// Skip TLS certificate verification (debugging only).
    pub insecure_tls: bool,
}

/// `s3://bucket` or `s3://bucket/prefix`.
pub struct Target {
    pub store: Store,
    pub prefix: String,
}

pub fn target(uri: &str, c: &Conn) -> anyhow::Result<Target> {
    let (endpoint, region) = (c.endpoint.as_deref(), c.region.as_deref());
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
        allow_http: c.allow_http,
        insecure_tls: c.insecure_tls,
        sha256_checksums: false,

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
    /// Every manifest in the bucket, decoded.
    ///
    /// A manifest that cannot be read fails the command. Skipping it would
    /// make an archived backup disappear from `list`, `verify` and the chain
    /// walk while every command still reported success — and would let
    /// `retention` reason about chains it cannot see.
    pub async fn manifests(&self) -> anyhow::Result<Vec<Manifest>> {
        // Enumerate manifests without walking chunk objects. A hundred-TB
        // dataset holds millions of chunk objects; a flat list of the whole
        // prefix would return every one of them just to find the handful of
        // manifest.json files. A delimited walk visits only dataset dirs and
        // snapshot dirs, and each snapshot's manifest key is deterministic
        // from there.
        let root = keys::all_manifests_prefix(&self.prefix); // "…/zb/v1/"
        let mut manifest_keys = Vec::new();
        for ds_dir in self.store.list_dirs(&root).await? {
            // `pins/` sits alongside the dataset dirs but holds flat marker
            // objects, not snapshot subdirs, so it yields no manifest keys and
            // needs no special case.
            for snap_dir in self.store.list_dirs(&format!("{ds_dir}/")).await? {
                manifest_keys.push(format!("{snap_dir}/manifest.json"));
            }
        }

        // Fetch and decode in parallel. A snapshot dir that is mid-send has a
        // pending marker but no manifest yet, so a missing manifest is skipped;
        // an unreadable or wrong-version one fails the command (see above).
        let fetched: Vec<Option<Manifest>> = futures::stream::iter(manifest_keys)
            .map(|key| async move {
                match self.store.get(&key).await {
                    Ok(bytes) => {
                        let m = Manifest::decode(&bytes)
                            .map_err(|e| anyhow::anyhow!("{key}: manifest is unreadable: {e}"))?;
                        m.check_version()
                            .map_err(|e| anyhow::anyhow!("{key}: {e}"))?;
                        Ok::<_, anyhow::Error>(Some(m))
                    }
                    Err(crate::store::StoreError::NotFound(_)) => Ok(None),
                    Err(e) => Err(e.into()),
                }
            })
            .buffer_unordered(16)
            .try_collect()
            .await?;
        let mut out: Vec<Manifest> = fetched.into_iter().flatten().collect();
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
        pick(&all, snapshot).cloned()
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

/// The one manifest with this snapshot name. Snapshot names repeat across
/// datasets — two hosts sharing a bucket, or a dataset recreated under the
/// same name — while GUIDs do not, so an ambiguous name is an error naming
/// the candidates rather than a silent pick.
pub fn pick<'a>(all: &'a [Manifest], snapshot: &str) -> anyhow::Result<&'a Manifest> {
    let hits: Vec<&Manifest> = all.iter().filter(|m| m.snapshot == snapshot).collect();
    match hits.len() {
        0 => bail!("{snapshot} is not archived here (see `list`)"),
        1 => Ok(hits[0]),
        _ => {
            let mut msg = format!("{snapshot} matches {} archived snapshots:\n", hits.len());
            for m in &hits {
                msg.push_str(&format!(
                    "  dataset {} (guid {}), snapshot guid {}, {} bytes, {}\n",
                    m.dataset,
                    m.dataset_guid,
                    m.snapshot_guid,
                    m.bytes,
                    m.created_at
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap_or_else(|_| m.created_at.to_string()),
                ));
            }
            msg.push_str("give each host its own prefix (s3://bucket/<host>) to keep them apart");
            bail!(msg)
        }
    }
}

/// Retry a fallible upload/download step with capped exponential backoff.
///
/// Store errors that say what went wrong — bad credentials, a missing
/// bucket, a rejected checksum — are returned immediately; retrying those
/// only delays the message the operator needs.
pub async fn retry<T, F, Fut>(what: &str, mut op: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let mut attempt = 0u32;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e)
                if e.downcast_ref::<crate::store::StoreError>()
                    .is_some_and(|s| !s.is_retryable()) =>
            {
                return Err(e.context(format!("{what}: not retryable")));
            }
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

#[cfg(test)]
mod tests {
    use super::pick;
    use crate::manifest::{FORMAT_VERSION, Manifest};
    use crate::types::Guid;

    fn m(ds: u64, snap: u64, name: &str) -> Manifest {
        Manifest {
            format_version: FORMAT_VERSION,
            dataset: format!("tank/d{ds}"),
            dataset_guid: Guid(ds),
            snapshot: name.into(),
            snapshot_guid: Guid(snap),
            from_guid: None,
            createtxg: 1,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
            send_flags: Default::default(),
            bytes: 0,
            stream_blake3: String::new(),
            end_checksum: None,
            chunks: Vec::new(),
        }
    }

    #[test]
    fn unique_name_resolves() {
        let all = vec![m(1, 10, "tank/data@a"), m(1, 11, "tank/data@b")];
        assert_eq!(pick(&all, "tank/data@b").unwrap().snapshot_guid, Guid(11));
    }

    #[test]
    fn missing_name_errors() {
        assert!(pick(&[m(1, 10, "tank/data@a")], "tank/data@z").is_err());
    }

    /// Two hosts backing up the same dataset name into one bucket: the tool
    /// must refuse rather than guess which host's data is meant.
    #[test]
    fn ambiguous_name_errors_with_candidates() {
        let all = vec![m(1, 10, "tank/data@daily"), m(2, 20, "tank/data@daily")];
        let e = pick(&all, "tank/data@daily").unwrap_err().to_string();
        assert!(e.contains("matches 2"), "{e}");
        assert!(
            e.contains(&Guid(1).to_string()) && e.contains(&Guid(2).to_string()),
            "{e}"
        );
    }
}
