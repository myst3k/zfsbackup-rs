//! Thin, typed wrapper around the `zfs` command-line tool.
//!
//! There is no maintained libzfs binding for Rust, and the CLI is the stable,
//! documented interface anyway. Every call:
//!
//! - uses machine-readable output (`-H -p`) and parses it strictly;
//! - captures stderr and turns it into a typed error — nothing ever hangs on
//!   a failed subprocess;
//! - is idempotent where ZFS allows it (`hold`/`release`/`bookmark` treat
//!   "already exists" / "does not exist" as success).

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use crate::types::Guid;
use crate::types::SendFlags;
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStdout, Command};
use tokio::task::JoinHandle;
use tracing::{debug, instrument, warn};

#[derive(Debug, Error)]
pub enum ZfsError {
    #[error("failed to run {cmd}: {source}")]
    Spawn {
        cmd: String,
        #[source]
        source: std::io::Error,
    },
    #[error("`zfs {args}` failed (exit {code:?}): {stderr}")]
    Failed {
        args: String,
        code: Option<i32>,
        stderr: String,
    },
    #[error("could not parse `zfs {args}` output: {line:?}")]
    Parse { args: String, line: String },
    #[error("dataset or snapshot not found: {0}")]
    NotFound(String),
    #[error("zfs did not respond within {0:?}")]
    Timeout(Duration),
}

impl ZfsError {
    /// Errors that mean "the thing isn't there" rather than "something broke".
    pub fn is_not_found(&self) -> bool {
        match self {
            ZfsError::NotFound(_) => true,
            ZfsError::Failed { stderr, .. } => {
                stderr.contains("does not exist") || stderr.contains("dataset does not exist")
            }
            _ => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, ZfsError>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Snapshot {
    /// Full name `pool/ds@snap`.
    pub name: String,
    pub dataset: String,
    pub snapname: String,
    pub guid: Guid,
    pub createtxg: u64,
    /// Unix seconds.
    pub creation: i64,
    /// Bytes referenced.
    pub referenced: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Dataset {
    pub name: String,
    pub guid: Guid,
    pub encrypted: bool,
    pub compression: String,
    pub recordsize: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bookmark {
    /// Full name `pool/ds#mark`.
    pub name: String,
    pub guid: Guid,
    pub createtxg: u64,
}

/// What to send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SendSpec {
    /// `pool/ds@snap`
    pub to: String,
    /// Incremental base: a snapshot (`@s`) or bookmark (`#b`) name, either
    /// short (`@s`) or full (`pool/ds@s`).
    pub from: Option<String>,
    pub flags: SendFlags,
}

impl SendSpec {
    pub fn args(&self) -> Vec<String> {
        let mut a: Vec<String> = vec!["send".into()];
        a.extend(self.flags.zfs_args().iter().map(|s| s.to_string()));
        if let Some(from) = &self.from {
            a.push("-i".into());
            a.push(from.clone());
        }
        a.push(self.to.clone());
        a
    }
}

/// A running `zfs send`. Read the stream from `stdout`; call [`SendProcess::wait`]
/// after EOF to learn whether zfs itself succeeded.
pub struct SendProcess {
    child: Child,
    stdout: Option<ChildStdout>,
    stderr: tokio::task::JoinHandle<String>,
}

impl SendProcess {
    /// Take the stream reader. Panics if called twice.
    pub fn take_stdout(&mut self) -> ChildStdout {
        self.stdout.take().expect("stdout already taken")
    }

    /// Wait for the process to exit; errors if zfs failed.
    pub async fn wait(mut self) -> Result<()> {
        wait_child(&mut self.child, self.stderr, "send").await
    }

    /// Kill the process (used when the consumer aborts).
    pub async fn kill(mut self) {
        let _ = self.child.kill().await;
    }
}

#[derive(Clone, Debug)]
pub struct Zfs {
    binary: PathBuf,
    /// Timeout for metadata commands (list/get/hold…). Sends are unbounded.
    timeout: Duration,
}

impl Default for Zfs {
    fn default() -> Self {
        Self::new()
    }
}

impl Zfs {
    pub fn new() -> Self {
        Self {
            binary: PathBuf::from("zfs"),
            timeout: Duration::from_secs(120),
        }
    }

    pub fn with_binary(mut self, path: impl Into<PathBuf>) -> Self {
        self.binary = path.into();
        self
    }

    pub fn with_timeout(mut self, t: Duration) -> Self {
        self.timeout = t;
        self
    }

    /// Run a metadata command and return stdout. Never hangs: bounded by
    /// `timeout`, stderr always drained.
    #[instrument(skip(self), level = "debug")]
    async fn run(&self, args: &[&str]) -> Result<String> {
        let joined = args.join(" ");
        let fut = Command::new(&self.binary)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .output();
        let out = tokio::time::timeout(self.timeout, fut)
            .await
            .map_err(|_| ZfsError::Timeout(self.timeout))?
            .map_err(|e| ZfsError::Spawn {
                cmd: format!("zfs {joined}"),
                source: e,
            })?;
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).into_owned())
        } else {
            let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
            if stderr.contains("does not exist") {
                return Err(ZfsError::NotFound(joined));
            }
            Err(ZfsError::Failed {
                args: joined,
                code: out.status.code(),
                stderr,
            })
        }
    }

    pub async fn version(&self) -> Result<String> {
        let out = self.run(&["version"]).await?;
        // First line looks like `zfs-2.3.1-1`.
        Ok(out.lines().next().unwrap_or("").trim().to_string())
    }

    pub async fn dataset(&self, name: &str) -> Result<Dataset> {
        let out = self
            .run(&[
                "get",
                "-Hp",
                "-o",
                "value",
                "guid,encryption,compression,recordsize",
                name,
            ])
            .await?;
        let v: Vec<&str> = out.lines().collect();
        if v.len() < 4 {
            return Err(ZfsError::Parse {
                args: format!("get … {name}"),
                line: out,
            });
        }
        Ok(Dataset {
            name: name.to_string(),
            guid: v[0].parse().map_err(|_| ZfsError::Parse {
                args: "get guid".into(),
                line: v[0].into(),
            })?,
            encrypted: v[1] != "off",
            compression: v[2].to_string(),
            recordsize: v[3].parse().map_err(|_| ZfsError::Parse {
                args: "get recordsize".into(),
                line: v[3].into(),
            })?,
        })
    }

    /// Snapshots of one dataset, oldest first.
    #[instrument(skip(self))]
    pub async fn snapshots(&self, dataset: &str) -> Result<Vec<Snapshot>> {
        let out = self
            .run(&[
                "list",
                "-Hp",
                "-t",
                "snapshot",
                "-s",
                "createtxg",
                "-o",
                "name,guid,createtxg,creation,referenced",
                "-d",
                "1",
                dataset,
            ])
            .await?;
        let mut v = Vec::new();
        for line in out.lines() {
            let f: Vec<&str> = line.split('\t').collect();
            let parse_err = || ZfsError::Parse {
                args: "list -t snapshot".into(),
                line: line.to_string(),
            };
            if f.len() != 5 {
                return Err(parse_err());
            }
            let (ds, snap) = f[0].split_once('@').ok_or_else(parse_err)?;
            v.push(Snapshot {
                name: f[0].to_string(),
                dataset: ds.to_string(),
                snapname: snap.to_string(),
                guid: f[1].parse().map_err(|_| parse_err())?,
                createtxg: f[2].parse().map_err(|_| parse_err())?,
                creation: f[3].parse().map_err(|_| parse_err())?,
                referenced: f[4].parse().map_err(|_| parse_err())?,
            });
        }
        Ok(v)
    }

    pub async fn snapshot(&self, name: &str) -> Result<Snapshot> {
        let (ds, _) = name
            .split_once('@')
            .ok_or_else(|| ZfsError::NotFound(name.into()))?;
        self.snapshots(ds)
            .await?
            .into_iter()
            .find(|s| s.name == name)
            .ok_or_else(|| ZfsError::NotFound(name.into()))
    }

    #[instrument(skip(self))]
    pub async fn bookmarks(&self, dataset: &str) -> Result<Vec<Bookmark>> {
        let out = self
            .run(&[
                "list",
                "-Hp",
                "-t",
                "bookmark",
                "-o",
                "name,guid,createtxg",
                "-d",
                "1",
                dataset,
            ])
            .await?;
        let mut v = Vec::new();
        for line in out.lines() {
            let f: Vec<&str> = line.split('\t').collect();
            let parse_err = || ZfsError::Parse {
                args: "list -t bookmark".into(),
                line: line.to_string(),
            };
            if f.len() != 3 {
                return Err(parse_err());
            }
            v.push(Bookmark {
                name: f[0].to_string(),
                guid: f[1].parse().map_err(|_| parse_err())?,
                createtxg: f[2].parse().map_err(|_| parse_err())?,
            });
        }
        Ok(v)
    }

    /// Size estimate for a send (`zfs send -nvP`). Unreliable with `-c`;
    /// use for progress only.
    pub async fn estimate(&self, spec: &SendSpec) -> Result<u64> {
        let mut args = spec.args();
        args.insert(1, "-nvP".into());
        let a: Vec<&str> = args.iter().map(String::as_str).collect();
        let out = self.run(&a).await?;
        // `-P` prints a line: `size\t<bytes>`.
        for line in out.lines() {
            if let Some(rest) = line.strip_prefix("size")
                && let Ok(n) = rest.trim().parse::<u64>()
            {
                return Ok(n);
            }
        }
        Err(ZfsError::Parse {
            args: a.join(" "),
            line: out,
        })
    }

    /// Spawn `zfs <args>` with stdout/stderr piped and stderr drained on a
    /// task, so a chatty zfs can never block on a full pipe.
    fn spawn_piped<S: AsRef<std::ffi::OsStr>>(
        &self,
        args: &[S],
        stdin: Stdio,
    ) -> Result<(Child, JoinHandle<String>)> {
        let cmd = || {
            let shown: Vec<String> = args
                .iter()
                .map(|a| a.as_ref().to_string_lossy().into_owned())
                .collect();
            format!("zfs {}", shown.join(" "))
        };
        let mut child = Command::new(&self.binary)
            .args(args)
            .stdin(stdin)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| ZfsError::Spawn {
                cmd: cmd(),
                source: e,
            })?;
        let mut stderr = child.stderr.take().ok_or_else(|| ZfsError::Spawn {
            cmd: cmd(),
            source: std::io::Error::other("stderr pipe not attached"),
        })?;
        let stderr_task = tokio::spawn(async move {
            let mut s = String::new();
            if let Err(e) = stderr.read_to_string(&mut s).await {
                s.push_str(&format!("<stderr unreadable: {e}>"));
            }
            s
        });
        Ok((child, stderr_task))
    }

    /// Start a `zfs send`. The returned stdout is the raw stream.
    #[instrument(skip(self))]
    pub async fn send(&self, spec: &SendSpec) -> Result<SendProcess> {
        let args = spec.args();
        let (mut child, stderr) = self.spawn_piped(&args, Stdio::null())?;
        let stdout = child.stdout.take().ok_or_else(|| ZfsError::Spawn {
            cmd: format!("zfs {}", args.join(" ")),
            source: std::io::Error::other("stdout pipe not attached"),
        })?;
        Ok(SendProcess {
            child,
            stdout: Some(stdout),
            stderr,
        })
    }

    /// `zfs hold <tag> <snapshot>`. Returns `true` if the hold was created
    /// now, `false` if it already existed (also success).
    #[instrument(skip(self))]
    pub async fn hold(&self, tag: &str, snapshot: &str) -> Result<bool> {
        match self.run(&["hold", tag, snapshot]).await {
            Ok(_) => Ok(true),
            Err(ZfsError::Failed { stderr, .. }) if stderr.contains("tag already exists") => {
                debug!(tag, snapshot, "hold already present");
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// `zfs release <tag> <snapshot>`; missing hold or snapshot is success.
    #[instrument(skip(self))]
    pub async fn release(&self, tag: &str, snapshot: &str) -> Result<()> {
        match self.run(&["release", tag, snapshot]).await {
            Ok(_) => Ok(()),
            Err(e) if e.is_not_found() => {
                debug!(tag, snapshot, "nothing to release");
                Ok(())
            }
            Err(ZfsError::Failed { stderr, .. }) if stderr.contains("no such tag") => Ok(()),
            Err(e) => Err(e),
        }
    }

    pub async fn holds(&self, snapshot: &str) -> Result<Vec<String>> {
        let out = self.run(&["holds", "-H", snapshot]).await?;
        Ok(out
            .lines()
            .filter_map(|l| l.split('\t').nth(1).map(str::to_string))
            .collect())
    }

    /// `zfs bookmark pool/ds@snap pool/ds#mark`; an existing bookmark is
    /// success. Bookmark names embed the snapshot GUID, so a name collision
    /// can only be the same snapshot. OpenZFS does not report EEXIST
    /// consistently (some versions exit 1 with nothing on stderr), so the
    /// failure path checks for the bookmark instead of parsing a message.
    #[instrument(skip(self))]
    pub async fn bookmark(&self, snapshot: &str, bookmark: &str) -> Result<()> {
        match self.run(&["bookmark", snapshot, bookmark]).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if self.bookmark_exists(bookmark).await? {
                    debug!(snapshot, bookmark, "bookmark already present");
                    Ok(())
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn bookmark_exists(&self, bookmark: &str) -> Result<bool> {
        match self
            .run(&["list", "-H", "-t", "bookmark", "-o", "name", bookmark])
            .await
        {
            Ok(out) => Ok(out.lines().any(|l| l.trim() == bookmark)),
            Err(e) if e.is_not_found() => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub async fn destroy_bookmark(&self, bookmark: &str) -> Result<()> {
        match self.run(&["destroy", bookmark]).await {
            Ok(_) => Ok(()),
            Err(e) if e.is_not_found() => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Start a `zfs receive -s -u [-F] <target>`; write the stream to stdin.
    /// `-u` leaves the dataset unmounted so no root privilege is needed;
    /// mount it afterwards with `zfs mount`.
    #[instrument(skip(self))]
    pub async fn receive(&self, target: &str, force: bool) -> Result<ReceiveProcess> {
        let mut args = vec!["receive", "-s", "-u"];
        if force {
            args.push("-F");
        }
        args.push(target);
        let (mut child, stderr) = self.spawn_piped(&args, Stdio::piped())?;
        let stdin = child.stdin.take().ok_or_else(|| ZfsError::Spawn {
            cmd: format!("zfs {}", args.join(" ")),
            source: std::io::Error::other("stdin pipe not attached"),
        })?;
        Ok(ReceiveProcess {
            child,
            stdin: Some(stdin),
            stderr,
        })
    }

    /// Abort a partially received stream (`zfs receive -A`).
    pub async fn receive_abort(&self, target: &str) -> Result<()> {
        match self.run(&["receive", "-A", target]).await {
            Ok(_) => Ok(()),
            Err(e) if e.is_not_found() => Ok(()),
            Err(ZfsError::Failed { stderr, .. })
                if stderr.contains("does not have any resumable receive state") =>
            {
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Feature flags enabled on a pool, for restore-target compatibility checks.
    pub async fn pool_features(&self, pool: &str) -> Result<Vec<String>> {
        let out = Command::new("zpool")
            .args(["get", "-Hp", "-o", "property,value", "all", pool])
            .stdin(Stdio::null())
            .output()
            .await
            .map_err(|e| ZfsError::Spawn {
                cmd: "zpool get".into(),
                source: e,
            })?;
        if !out.status.success() {
            warn!(pool, "zpool get failed");
            return Ok(Vec::new());
        }
        Ok(String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|l| {
                let (k, v) = l.split_once('\t')?;
                let k = k.strip_prefix("feature@")?;
                (v == "enabled" || v == "active").then(|| k.to_string())
            })
            .collect())
    }
}

/// A running `zfs receive`.
pub struct ReceiveProcess {
    child: Child,
    pub stdin: Option<tokio::process::ChildStdin>,
    stderr: tokio::task::JoinHandle<String>,
}

impl ReceiveProcess {
    /// Close stdin and wait for zfs to finish applying the stream.
    pub async fn finish(mut self) -> Result<()> {
        drop(self.stdin.take());
        wait_child(&mut self.child, self.stderr, "receive").await
    }
}

/// Wait for a piped zfs child and turn a non-zero exit into `ZfsError::Failed`
/// carrying its stderr.
async fn wait_child(child: &mut Child, stderr: JoinHandle<String>, what: &str) -> Result<()> {
    let status = child.wait().await.map_err(|e| ZfsError::Spawn {
        cmd: format!("zfs {what}"),
        source: e,
    })?;
    let stderr = match stderr.await {
        Ok(s) => s,
        Err(e) => format!("<stderr task failed: {e}>"),
    };
    if status.success() {
        if !stderr.trim().is_empty() {
            debug!(stderr = %stderr.trim(), "zfs {what} stderr");
        }
        Ok(())
    } else {
        Err(ZfsError::Failed {
            args: what.into(),
            code: status.code(),
            stderr: stderr.trim().to_string(),
        })
    }
}

/// Hold/bookmark naming, tagged with the job so multiple destinations never
/// clobber each other.
pub mod tags {
    use crate::types::Guid;

    pub fn hold(job: &str) -> String {
        format!("zb:{job}")
    }
    /// `pool/ds#zb_<job>_<guid>` for the given `pool/ds@snap`. One
    /// bookmark per committed snapshot; older ones are destroyed on commit.
    pub fn bookmark(job: &str, snapshot: &str, guid: Guid) -> String {
        let ds = snapshot.split('@').next().unwrap_or(snapshot);
        format!("{ds}#{}", bookmark_short(job, guid))
    }
    pub fn bookmark_short(job: &str, guid: Guid) -> String {
        format!("zb_{job}_{guid}")
    }
    /// Parse the guid out of a bookmark name we created, if it is ours.
    pub fn bookmark_guid(job: &str, name: &str) -> Option<Guid> {
        let short = name.rsplit('#').next()?;
        let rest = short.strip_prefix(&format!("zb_{job}_"))?;
        rest.parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_args() {
        let s = SendSpec {
            to: "tank/d@b".into(),
            from: Some("@a".into()),
            flags: SendFlags {
                raw: true,
                compressed: true,
                large_blocks: true,
                ..Default::default()
            },
        };
        assert_eq!(
            s.args(),
            vec!["send", "--raw", "-c", "-L", "-i", "@a", "tank/d@b"]
        );
    }

    #[test]
    fn tag_names() {
        assert_eq!(tags::hold("j1"), "zb:j1");
        let g = Guid(0xabc);
        let bm = tags::bookmark("j1", "tank/d@s", g);
        assert_eq!(bm, "tank/d#zb_j1_0000000000000abc");
        assert_eq!(tags::bookmark_guid("j1", &bm), Some(g));
        assert_eq!(tags::bookmark_guid("j2", &bm), None);
    }

    #[tokio::test]
    async fn missing_binary_is_typed_error() {
        let z = Zfs::new().with_binary("/nonexistent/zfs");
        let err = z.version().await.unwrap_err();
        assert!(matches!(err, ZfsError::Spawn { .. }), "{err}");
    }
}
