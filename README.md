# zfsbackup-rs

Back up ZFS snapshots to S3-compatible object storage. A single static
binary that streams `zfs send` straight to a bucket, verifies every byte end
to end, and manages incremental chains and retention with the bucket as the
only source of truth — no server, no daemon, no database.

[![ci](https://github.com/myst3k/zfsbackup-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/myst3k/zfsbackup-rs/actions/workflows/ci.yml)
[![license: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

> **Status: alpha (v0.1). Not yet recommended as your only backup.**

## Overview

`zfsbackup-rs` archives ZFS snapshots to any S3-compatible store (AWS S3,
Wasabi, MinIO, Ceph). It runs `zfs send`, splits the stream into fixed-size
chunks, uploads them in parallel, and writes one JSON manifest per snapshot.
Restores replay the chunks back into `zfs receive`. Everything else — listing
backups, walking incremental chains, expiring old snapshots, garbage
collection — is derived from the manifests in the bucket, so a bucket is a
complete, self-describing backup: copy it and you have copied the backup
system.

Integrity is enforced at every stage:

- **Read** — the fletcher4 checksum ZFS embeds in each record is verified as
  the stream is chunked, and the stream's END checksum is recorded.
- **Write** — each chunk is uploaded with `x-amz-checksum-crc32c`, which the
  store verifies server-side and rejects on mismatch. Stores that ignore the
  header fall back to plain PUTs, with integrity then covered by the manifest.
- **At rest** — the manifest records the size and BLAKE3 of every chunk and of
  the whole stream.
- **Restore** — each chunk is BLAKE3-checked before it reaches `zfs receive`,
  and ZFS re-validates the stream on the way in.

`verify` re-downloads and re-hashes a backup without ZFS or the source pool,
so integrity can be audited from anywhere on a schedule.

## Install

Static musl binaries for x86-64 and arm64 Linux are attached to each
[release](https://github.com/myst3k/zfsbackup-rs/releases):

```sh
curl -fsSL https://github.com/myst3k/zfsbackup-rs/releases/latest/download/zfsbackup-rs-v0.1.0-x86_64-unknown-linux-musl.tar.gz | tar xz
sudo install -m755 zfsbackup-rs-*/zfsbackup-rs /usr/local/bin/
```

From source (Rust 1.85+, edition 2024):

```sh
cargo install --git https://github.com/myst3k/zfsbackup-rs
```

`send` and `receive` require the `zfs` binary and either root or the relevant
`zfs allow` delegations. `list`, `verify`, `retention`, `check` and `clean`
need only the S3 credentials and run anywhere.

## Commands

Every command also accepts the global flags in [Configuration](#configuration)
(`--endpoint`, `--region`, `--allow-http`, `--insecure-tls`, `--zfs`) and their
environment equivalents; the table lists only command-specific flags. Run
`zfsbackup-rs <command> --help` for the canonical reference.

| Command | Description | Flags |
|---|---|---|
| `send <snapshot> <uri>` | Archive one snapshot. With no base it sends a full stream; otherwise it uses the newest already-archived snapshot of the dataset as the incremental base. Runs `zfs send -c -L [--raw] [-i <base>]`, chunks the stream, uploads it, and commits a manifest. Interrupted runs resume. | `--from <@snap\|#bookmark>` — explicit incremental base (must be archived)<br>`--full` — force a full even when a base exists<br>`--chunk-size <size>` — part size, 5MiB–5GiB (default `64MiB`)<br>`--parallel <n>` — concurrent chunks (default `4`); peak mem ≈ chunk-size × (n+1) |
| `receive <snapshot> <uri> <target>` | Restore the snapshot and the whole chain it depends on into `<target>`, oldest first, via `zfs receive -s -u`. Each chunk is BLAKE3-verified before it reaches ZFS. Target is left **unmounted** — `zfs mount <target>` after. | `--force` — pass `-F` to `zfs receive` (roll back / overwrite)<br>`--window <n>` — chunks prefetched (default `4`); peak mem ≈ n × the sender's chunk size |
| `list <uri>` | List archived snapshots — kind (full / incremental + base), size, chunk count, pins — from the manifests alone. | `--dataset <name>` — only this dataset (trailing `*` = prefix) |
| `verify <snapshot> <uri>` | Re-download every chunk and check its size and BLAKE3, plus the whole-stream BLAKE3, against the manifest. Writes nothing; needs no ZFS or source pool. | *(none)* |
| `retention <uri>` | Delete snapshots outside the policy, never breaking a chain or touching pins. Needs at least one of `--older-than` / `--keep-last`. Plain S3 DELETEs, so a versioned bucket keeps prior versions. | `--older-than <dur>` — delete older than e.g. `90d`, `12w`, `24h` (needed ancestors kept)<br>`--keep-last <n>` — keep newest N per dataset; `0` needs an age policy<br>`--dataset <name>` — scope to one dataset (trailing `*` = prefix)<br>`--dry-run` — print the plan, delete nothing |
| `check <uri>` | Probe an endpoint before trusting it: reachability, credentials, bucket, versioning, Object Lock, lifecycle, a read/write/delete round trip, and whether it truly verifies upload checksums (uploads a deliberately wrong CRC32C and confirms it is rejected). | *(none)* |
| `clean <uri>` | Remove objects no manifest references — chunks from a send that never committed, and strays beside a manifest. Sends holding a live lease are skipped. | `--dry-run` — print what would be removed, remove nothing |
| `pin <snapshot> <uri>`<br>`unpin <snapshot> <uri>` | Exempt a snapshot (and, through the retention rules, its whole ancestry) from deletion, or lift that. A pin is a marker object in the bucket. | *(none)* |

## Usage

Set credentials and endpoint once (see [Configuration](#configuration) for all
inputs):

```sh
export AWS_ACCESS_KEY_ID=…  AWS_SECRET_ACCESS_KEY=…
export ZB_ENDPOINT=https://s3.us-east-2.wasabisys.com
export ZB_REGION=us-east-2
```

**Confirm the bucket is fit for backups** — do this once per endpoint:

```sh
zfsbackup-rs check s3://backups
```

**First backup of a dataset** (a full stream). You take the snapshot; the tool
sends it:

```sh
zfs snapshot tank/data@2026-09-01
zfsbackup-rs send tank/data@2026-09-01 s3://backups
# runs: zfs send -c -L tank/data@2026-09-01  → chunked → uploaded → manifest
```

**Later backups** (incremental from the previous one, chosen automatically):

```sh
zfs snapshot tank/data@2026-09-02
zfsbackup-rs send tank/data@2026-09-02 s3://backups
# runs: zfs send -c -L -i tank/data@2026-09-01 tank/data@2026-09-02
```

**Give each host its own prefix** so their datasets never collide:

```sh
zfsbackup-rs send tank/data@2026-09-02 s3://backups/hosts/alpha
```

**List and verify** what's stored (no ZFS required — run it from anywhere):

```sh
zfsbackup-rs list   s3://backups
zfsbackup-rs verify tank/data@2026-09-02 s3://backups
```

**Restore** the snapshot and its full chain into a new dataset, then mount it:

```sh
zfsbackup-rs receive tank/data@2026-09-02 s3://backups tank/restore
sudo zfs mount tank/restore
# uses: zfs receive -s -u tank/restore   (unmounted; hence the explicit mount)
```

**Expire old backups** — keep the last 30 and anything under 90 days, chain-safe.
Preview first:

```sh
zfsbackup-rs retention s3://backups --keep-last 30 --older-than 90d --dry-run
zfsbackup-rs retention s3://backups --keep-last 30 --older-than 90d
```

**Protect a snapshot** from retention (e.g. a monthly you want to keep):

```sh
zfsbackup-rs pin tank/data@2026-09-01 s3://backups
```

**Reclaim orphaned objects** left by interrupted sends:

```sh
zfsbackup-rs clean s3://backups --dry-run
zfsbackup-rs clean s3://backups
```

A typical cron entry — snapshot, back up incrementally, prune:

```sh
0 2 * * *  snap=tank/data@$(date -u +\%Y-\%m-\%dT\%H:\%M:\%SZ); \
  zfs snapshot "$snap" && \
  zfsbackup-rs send "$snap" s3://backups/hosts/alpha && \
  zfsbackup-rs retention s3://backups/hosts/alpha --keep-last 30 --older-than 90d
```

## How it works

### Bucket layout

Objects are written under a versioned, GUID-keyed prefix:

```text
zb/v1/<dataset-guid>/<snapshot-guid>/manifest.json
zb/v1/<dataset-guid>/<snapshot-guid>/chunk-000000 … chunk-NNNNNN
zb/v1/pins/<snapshot-guid>
```

Keys use ZFS object GUIDs rather than names, so renaming a dataset never
orphans its history; human-readable names live inside the manifests. The `zb/`
prefix namespaces the tool's objects so a bucket can hold unrelated data, and
`v1` versions the format — a manifest written by a newer, incompatible version
is refused rather than misread.

The manifest is written **last**, after every chunk is uploaded, so its
presence is the commit point: an interrupted `send` leaves only orphaned
chunks (invisible to every command, reclaimable by `clean`), never a
half-listed backup. `retention` deletes the manifest **first**, so a listed
backup always has all of its chunks.

### Incrementals

After a successful send the tool leaves a ZFS bookmark, so the base snapshot
can be destroyed locally and later incrementals still resolve. `send` picks a
base from the most recent archived snapshot that still exists locally (as a
snapshot or bookmark), ordered by archive time rather than pool-local
`createtxg` so the choice survives a restore onto a different pool. `receive`
walks the `from`-links back to the full and replays the chain in order,
refusing one with a missing link.

### Resume and concurrency

A `send` writes a lease marker before uploading; a second send of the same
snapshot sees a live lease and steps aside instead of corrupting the first.
An interrupted send reuses chunks already uploaded when the next run produces
an identical stream (same base, flags and chunk size), and starts clean
otherwise.

### Retention

A snapshot survives if it falls within `--older-than`, ranks among the newest
`--keep-last` of its dataset, is pinned, or is an ancestor of anything that
survives. Deletions use plain S3 DELETEs, so on a versioned bucket the prior
versions remain as an undo; erasing unreferenced objects outright is `clean`'s
job.

## Configuration

Credentials come from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`. Endpoint
and region come from `--endpoint` / `--region`, `ZB_ENDPOINT` / `ZB_REGION`,
or `AWS_ENDPOINT_URL` / `AWS_REGION`.

| Variable / flag | Effect |
|---|---|
| `--allow-http`, `ZB_ALLOW_HTTP=1` | Permit a plain `http://` endpoint. Credentials and data travel unencrypted — trusted networks only. |
| `--insecure-tls`, `ZB_INSECURE_TLS=1` | Skip TLS certificate verification. Debugging only. |
| `--zfs`, `ZB_ZFS` | Path to the `zfs` binary (default `zfs`). |
| `RUST_LOG` | Log filter (`tracing_subscriber`), e.g. `RUST_LOG=debug`. |

Encrypted datasets are sent `--raw` automatically, so only ciphertext leaves
the host.

## Development

```sh
cargo test                    # unit tests
cargo clippy --all-targets    # lints (CI runs with -D warnings)
```

CI runs fmt, clippy and tests, then an end-to-end job that creates a ZFS pool
on a file vdev, starts a MinIO container, and drives the built binary through
send → incremental → verify → receive (byte-compared) → retention → clean on
every push. Tagging `v*` builds and publishes the release binaries.

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at
your option.
