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

## Usage

```sh
export AWS_ACCESS_KEY_ID=…  AWS_SECRET_ACCESS_KEY=…
export ZB_ENDPOINT=https://s3.us-east-2.wasabisys.com  ZB_REGION=us-east-2

zfsbackup-rs check   s3://backups                          # validate the endpoint once

zfs snapshot tank/data@$(date -u +%Y%m%dT%H%M%SZ)
zfsbackup-rs send    tank/data@… s3://backups              # first run full, then incremental

zfsbackup-rs list    s3://backups
zfsbackup-rs verify  tank/data@… s3://backups
zfsbackup-rs receive tank/data@… s3://backups tank/restore  # restores the whole chain
zfsbackup-rs retention s3://backups --keep-last 30 --older-than 90d
```

A path after the bucket confines everything to a prefix — give each host its
own, e.g. `s3://backups/hosts/alpha`.

### Commands

| Command | Description |
|---|---|
| `send <snap> <uri>` | Archive a snapshot. Selects the newest archived base for an incremental automatically; `--from` overrides it, `--full` forces a full. `--chunk-size` (default 64MiB, 5MiB–5GiB) and `--parallel` (default 4) tune throughput and memory. Interrupted sends resume. |
| `receive <snap> <uri> <dataset>` | Restore the snapshot and the full chain it depends on, oldest first. The dataset is left unmounted (`zfs receive -u`); `zfs mount` it after. `--force` passes `-F`; `--window` sets prefetch depth. |
| `list <uri>` | List archived snapshots with kind, size, chunk count and pins. `--dataset` filters (trailing `*` = prefix). |
| `verify <snap> <uri>` | Re-download every chunk and check it against the manifest. Writes nothing; needs no ZFS. |
| `retention <uri>` | Delete snapshots outside `--older-than` / `--keep-last`, never breaking a chain or touching pins. `--dataset` scopes the run; `--dry-run` shows the plan. |
| `check <uri>` | Probe an endpoint: reachability, credentials, versioning, Object Lock, lifecycle, read/write/delete, and whether it actually verifies upload checksums. |
| `clean <uri>` | Remove objects no manifest references — abandoned sends and strays. Skips sends holding a live lease. `--dry-run` shows the plan. |
| `pin` / `unpin <snap> <uri>` | Exempt a snapshot (and its chain) from retention, or lift that. |

Run `zfsbackup-rs <command> --help` for the full flag list.

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
| `ZB_INSECURE_TLS=1` | Skip TLS certificate verification. Debugging only. |
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
