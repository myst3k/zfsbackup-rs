//! zfsbackup-rs: zfs send streams to S3-compatible storage.
//!
//! One binary, no server: the bucket (chunks + manifests) is the source of
//! truth. Streams are verified three ways — the ZFS stream's own fletcher4
//! record checksums while reading, BLAKE3 per chunk in the manifest, and
//! CRC32C verified server-side on upload.

mod cmd;
mod fletcher;
mod manifest;
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
    /// Allow a plain-http:// endpoint (a MinIO or Ceph on a trusted
    /// network). Credentials and data travel unencrypted. Also settable as
    /// ZB_ALLOW_HTTP=1.
    #[arg(long, global = true)]
    allow_http: bool,
    /// Skip TLS certificate verification (debugging only, e.g. behind an
    /// intercepting proxy). Also settable as ZB_INSECURE_TLS=1.
    #[arg(long, global = true)]
    insecure_tls: bool,
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
        /// Chunks uploaded in parallel. Peak memory is roughly
        /// chunk-size × (parallel + 1): the uploads in flight plus the one
        /// being read from the pipe.
        #[arg(long, default_value_t = 4)]
        parallel: usize,
    },
    /// Restore a snapshot (and the chain it depends on) into a dataset.
    /// The dataset is left unmounted; `zfs mount` it afterwards.
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
        /// Maximum chunks prefetched ahead of the writer. Prefetch is
        /// adaptive and rarely reaches this; it is the ceiling, and it is
        /// capped so it never buffers more than ~512 MiB regardless of the
        /// sender's chunk size.
        #[arg(long, default_value_t = 16)]
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
        /// Keep the newest N snapshots per dataset and delete the rest.
        /// Combined with --older-than, both are kept. 0 keeps none by count
        /// and requires --older-than.
        #[arg(long)]
        keep_last: Option<usize>,
        /// Limit the run to this dataset (trailing '*' matches a prefix).
        /// Without it, every dataset in the bucket or prefix is in scope.
        #[arg(long)]
        dataset: Option<String>,
        /// Show what would be deleted without deleting.
        #[arg(long)]
        dry_run: bool,
    },
    /// Check that an endpoint and bucket behave the way backups need:
    /// credentials, versioning, and whether uploads are checksum-verified.
    Check {
        /// s3://bucket
        uri: String,
    },
    /// Remove objects no backup refers to: abandoned sends and stray chunks.
    Clean {
        /// s3://bucket
        uri: String,
        /// Show what would be removed without removing.
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

async fn run(cli: Cli) -> anyhow::Result<()> {
    let conn = cmd::Conn {
        endpoint: cli.endpoint.clone(),
        region: cli.region.clone(),
        allow_http: cli.allow_http || types::env_enabled("ZB_ALLOW_HTTP"),
        insecure_tls: cli.insecure_tls || types::env_enabled("ZB_INSECURE_TLS"),
    };
    match cli.cmd {
        Cmd::Send {
            snapshot,
            uri,
            from,
            full,
            chunk_size,
            parallel,
        } => {
            cmd::send::run(cmd::send::Args {
                snapshot,
                uri,
                from,
                full,
                chunk_size: types::parse_size(&chunk_size).map_err(anyhow::Error::msg)?,
                parallel,
                conn,
                zfs_bin: cli.zfs.clone(),
            })
            .await
        }
        Cmd::Receive {
            snapshot,
            uri,
            target,
            force,
            window,
        } => cmd::receive::run(&snapshot, &uri, &target, force, window, &conn, &cli.zfs).await,
        Cmd::List { uri, dataset } => cmd::list::run(&uri, dataset.as_deref(), &conn).await,
        Cmd::Verify { snapshot, uri } => cmd::verify::run(&snapshot, &uri, &conn).await,
        Cmd::Retention {
            uri,
            older_than,
            keep_last,
            dataset,
            dry_run,
        } => {
            cmd::retention::run(
                &uri,
                older_than.as_deref(),
                keep_last,
                dataset.as_deref(),
                dry_run,
                &conn,
            )
            .await
        }
        Cmd::Check { uri } => cmd::check::run(&uri, &conn).await,
        Cmd::Clean { uri, dry_run } => cmd::clean::run(&uri, dry_run, &conn).await,
        Cmd::Pin { snapshot, uri } => cmd::pin::run(&snapshot, &uri, true, &conn).await,
        Cmd::Unpin { snapshot, uri } => cmd::pin::run(&snapshot, &uri, false, &conn).await,
    }
}
