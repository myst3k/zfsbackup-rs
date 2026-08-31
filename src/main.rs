//! zfsbackup-rs: zfs send streams to S3-compatible storage.
//!
//! One binary, no server: the bucket (chunks + manifests) is the source of
//! truth. Streams are verified three ways — the ZFS stream's own fletcher4
//! record checksums while reading, BLAKE3 per chunk in the manifest, and
//! CRC32C verified server-side on upload.

mod fletcher;
mod hash;
mod store;
mod stream;
mod types;
mod zfs;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "zfsbackup-rs", version, about)]
struct Cli {
    /// S3 endpoint, e.g. https://s3.us-east-2.wasabisys.com
    #[arg(long, env = "ZB_ENDPOINT", global = true)]
    endpoint: Option<String>,
    /// S3 region.
    #[arg(long, env = "ZB_REGION", global = true)]
    region: Option<String>,
    /// Path to the zfs binary.
    #[arg(long, env = "ZB_ZFS", default_value = "zfs", global = true)]
    zfs: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Back up one snapshot (full, or incremental from the newest archived base).
    Send {
        /// pool/dataset@snapshot
        snapshot: String,
        /// s3://bucket
        uri: String,
        /// Incremental base (@snap or #bookmark); omit to pick automatically.
        #[arg(long)]
        from: Option<String>,
        /// Force a full send even when a base exists.
        #[arg(long)]
        full: bool,
        /// Chunk size, e.g. 64MiB (min 5MiB).
        #[arg(long, default_value = "64MiB")]
        chunk_size: String,
        /// Chunks uploaded in parallel.
        #[arg(long, default_value_t = 4)]
        parallel: usize,
    },
    /// Restore a snapshot (and the chain it depends on) into a dataset.
    Receive {
        /// pool/dataset@snapshot as recorded in the bucket
        snapshot: String,
        /// s3://bucket
        uri: String,
        /// Target dataset for zfs receive.
        target: String,
        /// Pass -F to zfs receive (rollback/overwrite target).
        #[arg(long)]
        force: bool,
        /// Chunks fetched ahead of the writer.
        #[arg(long, default_value_t = 4)]
        window: usize,
    },
    /// List archived snapshots.
    List {
        /// s3://bucket
        uri: String,
        /// Only this dataset (prefix match with a trailing *).
        #[arg(long)]
        dataset: Option<String>,
    },
    /// Download and re-hash every chunk of a snapshot; write nothing.
    Verify {
        /// pool/dataset@snapshot
        snapshot: String,
        /// s3://bucket
        uri: String,
    },
    /// Expire and delete archived snapshots that nothing depends on.
    Retention {
        /// s3://bucket
        uri: String,
        /// Delete snapshots older than this (e.g. 90d, 12w). Chains stay intact.
        #[arg(long)]
        older_than: Option<String>,
        /// Always keep the newest N snapshots per dataset.
        #[arg(long, default_value_t = 1)]
        keep_last: usize,
        /// Show what would be deleted without deleting.
        #[arg(long)]
        dry_run: bool,
    },
    /// Pin a snapshot: exclude it and its chain from retention.
    Pin { snapshot: String, uri: String },
    /// Remove a pin.
    Unpin { snapshot: String, uri: String },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(run(cli))
}

#[allow(unused_variables)]
async fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.cmd {
        _ => anyhow::bail!("not implemented yet"),
    }
}
