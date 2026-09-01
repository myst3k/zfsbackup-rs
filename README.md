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

Prebuilt Linux binaries for x86-64 and arm64 (dynamically linked against
glibc 2.28+, so they run on any distro from roughly 2018 on) are attached to
each [release](https://github.com/myst3k/zfsbackup-rs/releases):

```sh
curl -fsSL https://github.com/myst3k/zfsbackup-rs/releases/latest/download/zfsbackup-rs-v0.1.0-x86_64-unknown-linux-gnu.tar.gz | tar xz
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

| Command | Flags | Description |
|---|---|---|
| `send <snapshot> <uri>` | | Archive one snapshot. With no base it sends a full stream; otherwise it uses the newest already-archived snapshot of the dataset as the incremental base. Runs `zfs send -c -L [--raw] [-i <base>]`, chunks the stream, uploads it, and commits a manifest. Interrupted runs resume. |
| | `--from <@snap\|#bookmark>` | Explicit incremental base (must be archived). |
| | `--full` | Force a full send even when a base exists. |
| | `--chunk-size <size>` | Part size, 5MiB–5GiB (default `64MiB`). |
| | `--adaptive-chunk-size` | Pick the chunk size from the estimated stream size (aims for ~1000 chunks), overriding `--chunk-size`. |
| | `--adaptive-chunk-min <size>` | Lower bound for adaptive sizing (default `16MiB`). |
| | `--adaptive-chunk-max <size>` | Upper bound for adaptive sizing (default `512MiB`; raise for very large pools). |
| | `--parallel <n>` | Concurrent chunks (default `4`); peak memory ≈ chunk-size × (n+1). |
| `receive <snapshot> <uri> <target>` | | Restore the snapshot and the whole chain it depends on into `<target>`, oldest first, via `zfs receive -s -u`. Each chunk is BLAKE3-verified before it reaches ZFS. The target is left unmounted — `zfs mount <target>` after. |
| | `--force` | Pass `-F` to `zfs receive` (roll back / overwrite the target). |
| | `--window <n>` | Maximum chunks prefetched ahead of the writer (default `16`). Prefetch is adaptive and rarely reaches this; it's the ceiling, also capped so it never buffers more than ~512 MiB. |
| `list <uri>` | | List archived snapshots — kind (full / incremental + base), size, chunk count, pins — from the manifests alone. |
| | `--dataset <name>` | Only this dataset (trailing `*` matches a prefix). |
| `verify <snapshot> <uri>` | | Re-download every chunk and check its size and BLAKE3, plus the whole-stream BLAKE3, against the manifest. Writes nothing; needs no ZFS or source pool. |
| `retention <uri>` | | Delete snapshots outside the policy, never breaking a chain or touching pins. Needs at least one of `--older-than` / `--keep-last`. Plain S3 DELETEs, so a versioned bucket keeps prior versions. |
| | `--older-than <dur>` | Delete snapshots older than e.g. `90d`, `12w`, `24h`; ancestors a survivor needs are kept. |
| | `--keep-last <n>` | Keep the newest N per dataset; `0` requires an age policy. |
| | `--dataset <name>` | Scope the run to one dataset (trailing `*` matches a prefix). |
| | `--dry-run` | Print the plan; delete nothing. |
| `check <uri>` | | Probe an endpoint before trusting it: reachability, credentials, bucket, versioning, Object Lock, lifecycle, a read/write/delete round trip, and whether it truly verifies upload checksums (uploads a deliberately wrong CRC32C and confirms it is rejected). |
| `clean <uri>` | | Remove objects no manifest references — chunks from a send that never committed, and strays beside a manifest. Sends holding a live lease are skipped. |
| | `--dry-run` | Print what would be removed; remove nothing. |
| `pin <snapshot> <uri>` | | Exempt a snapshot (and, through the retention rules, its whole ancestry) from deletion. A pin is a marker object in the bucket. |
| `unpin <snapshot> <uri>` | | Remove a pin. |

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

### Choosing a chunk size

Each chunk is one S3 object (one PutObject), so the trade-off is round trips
and object count against memory and retry cost. Guidelines:

- **Too small** — under ~16 MiB, the per-request overhead (a round trip and a
  signature each) starts to dominate on a WAN, and the manifest and object
  count grow.
- **Too large** — memory is `chunk-size × (parallel + 1)` on send and up to the
  prefetch budget on restore, a failed chunk re-uploads more, and an
  interrupted send loses up to one chunk of progress.
- **Sweet spot** — 64–256 MiB suits most datasets; 64 MiB (the default) is fine
  up to a few hundred GB.

`--adaptive-chunk-size` removes the guesswork: it reads the estimated stream
size and picks a chunk size targeting ~1000 chunks, clamped to
`--adaptive-chunk-min` / `--adaptive-chunk-max` (16 MiB–512 MiB by default). So
a hundred-TB pool lands at the ceiling while a small incremental drops toward
the floor, and the object count stays roughly constant either way. On very
large pools, raise `--adaptive-chunk-max` (e.g. `1GiB`) and set `--parallel` so
that `max × (parallel + 1)` fits the host's RAM.

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

### Adaptive restore prefetch

`receive` reads chunks ahead of `zfs receive` so the pool never waits on the
network, but the right amount of read-ahead depends on the link and the pool,
and both vary. Rather than make you tune it, the prefetch depth adjusts itself:
each time the writer needs the next chunk, the tool notes whether that chunk
was already downloaded or the writer had to wait. If it had to wait, downloads
are the bottleneck and it fetches one more chunk ahead; if the chunk was
already sitting there, the pool is the bottleneck and it eases the depth back
down. A fast link climbs toward the ceiling on its own; a slow writer settles
near two in flight and stops buffering data it can't consume.

`--window` is the ceiling (default 16), not a fixed depth, and it's capped so
prefetch never holds more than ~512 MiB regardless of the sender's chunk size.
Chunks are always applied in order, whichever download finishes first.

### Verifying a backup

`verify` reads back every chunk of a snapshot and recomputes its BLAKE3, then
the whole-stream BLAKE3, checking both against the manifest. Because it hashes
the actual bytes, it depends on nothing but the data itself — a mismatch means
the stored chunk is corrupt. It needs no ZFS and no source pool, so it runs
anywhere the bucket credentials reach; a full run reads the entire stream.

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

CI runs fmt, clippy, tests and a release build on every push. Full end-to-end
runs (send → incremental → verify → receive with a byte-compare → retention →
clean) are exercised against a real ZFS pool and an S3-compatible bucket
outside CI. Tagging `v*` builds and publishes the release binaries.

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT) at
your option.
