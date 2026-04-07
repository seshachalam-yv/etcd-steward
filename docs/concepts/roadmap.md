<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# Roadmap

This document tracks what remains to be implemented relative to the full requirements in [gardener/etcd-steward#1](https://github.com/gardener/etcd-steward/issues/1).

## v0.1.0 — Alpha (current)

The current implementation is sufficient to run etcd-steward as a drop-in replacement for etcd-backup-restore in clusters using the **local filesystem** backup provider.

**Completed:**
- HTTP server with all etcd-wrapper endpoints (#2)
- etcd lock mechanism (#4)
- Member lease heartbeat (#16)
- Alarm handling — NOSPACE remediation (#6)
- Snapshotting — full + delta, cron + on-demand (#9)
- Compression — gzip + zstd (#8)
- Snapshot garbage collection — count-based (#10)
- etcd bootstrapping — paths A/B/C/D (#14)
- EtcdMember state transition updates (#15)
- Leadership observation (#3)

---

## Near-term (v0.1.x)

### Cloud snapstore providers ([#7](https://github.com/gardener/etcd-steward/issues/7))

Implement S3, GCS, and ABS snapstore providers. Each provider needs:
- `pkg/snapstore/s3.go` implementing `Snapstore`
- `pkg/snapstore/gcs.go`
- `pkg/snapstore/abs.go`
- Credentials config (endpoint, region, credentials path/secret)
- Unit tests with a fake HTTP server or provider SDK mock

This is the primary blocker for production use with cloud-hosted etcd clusters.

### Delta snapshot replay ([#11](https://github.com/gardener/etcd-steward/issues/11) / [#9](https://github.com/gardener/etcd-steward/issues/9))

Apply incremental snapshots on top of the full snapshot during restoration. Requires:
- MVCC event replay onto the restored bbolt DB
- Or: use an embedded etcd to apply events, then snapshot the result

The `pkg/restoration` package has a `// TODO` stub for this.

### Scheduled coordinated defragmentation ([#5](https://github.com/gardener/etcd-steward/issues/5))

Implement a defragmentation component that:
1. Checks DB size; triggers if above a configured threshold
2. Uses `pkg/lock` to coordinate: only one member defrags at a time
3. Runs on a cron schedule (`--defrag-schedule`)

The `DefragSchedule` config field already exists; a new `pkg/defrag` package is needed.

### Time-based GC policy ([#10](https://github.com/gardener/etcd-steward/issues/10))

Add a `TimeBased` retention policy to `pkg/gc`: retain all snapshot sets created within the last N hours. Add a `--gc-max-age` flag.

---

## Medium-term (v0.2.0)

### Resumable restoration ([#11](https://github.com/gardener/etcd-steward/issues/11))

- Download snapshot files to a local staging area before applying
- On pod restart, detect already-downloaded files and resume from the last applied snapshot
- Track progress via a state file in the data directory

### Data validation improvements ([#13](https://github.com/gardener/etcd-steward/issues/13))

- Add revision check: compare DB revision with latest snapshot revision in store
- Single-node: if DB lags, trigger partial restoration instead of full
- Multi-node: skip revision check (members can lag for legitimate reasons)
- Structured error codes for all validation failure modes

### Full compaction pipeline ([#12](https://github.com/gardener/etcd-steward/issues/12))

The `compact` subcommand currently only calls etcd `Compact`. The full pipeline:
1. Restore the target snapshot set to a local embedded etcd
2. Compact to remove old revisions
3. Defragment to minimize DB size
4. Upload a new full snapshot to the store

Reuse `pkg/restoration` and `pkg/snapshotter` to keep the subcommand lean.

### EtcdMember async status fields ([#15](https://github.com/gardener/etcd-steward/issues/15))

Populate `lastFullSnapshot`, `lastDeltaSnapshot`, and `lastDefragmentation` fields in `EtcdMember.status` after each operation. These are async (non-blocking) updates.

### Leadership pub-sub model ([#3](https://github.com/gardener/etcd-steward/issues/3))

Replace the pull-based `IsLeader()` gate with a push-based subscriber model: when leadership changes, a non-buffered channel notification is sent to all registered subscribers. This allows components to react instantly rather than waiting for their next polling tick.

---

## Long-term (v0.3.0+)

### CORRUPT alarm recovery ([#6](https://github.com/gardener/etcd-steward/issues/6))

When a CORRUPT alarm is detected during running operation:
1. Delete the corrupted member's data directory
2. Trigger restoration or learner sync with the leader
3. Disarm the alarm after successful recovery

This requires careful coordination to avoid splitting the cluster.

### etcd-wrapper delta replay callback ([#11](https://github.com/gardener/etcd-steward/issues/11))

To keep steward's memory footprint small, delta replay should use etcd-wrapper's embedded etcd rather than starting one inside steward. Design: steward calls an API on etcd-wrapper to start an embedded etcd in restore mode, then applies deltas via the etcd client.

### Security hardening ([#2](https://github.com/gardener/etcd-steward/issues/2))

Add HTTP security headers when TLS is enabled: `Strict-Transport-Security`, `X-Content-Type-Options`, `X-Frame-Options`, `Content-Security-Policy`.

### Load and performance tests

Benchmark snapshot upload/download throughput, GC cycle duration, and initialization latency. Run on every PR to catch performance regressions.

### Documentation: user and operator guides for cloud providers

Once cloud snapstore providers are implemented, add:
- Configuration examples for S3, GCS, ABS
- Credential management patterns for each provider
- Provider-specific troubleshooting
