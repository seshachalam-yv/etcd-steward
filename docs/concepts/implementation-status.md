<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
SPDX-License-Identifier: Apache-2.0
-->

# Implementation Status

This document tracks the completion status of each feature relative to the requirements in [gardener/etcd-steward#1](https://github.com/gardener/etcd-steward/issues/1).

## Feature matrix

| Issue | Feature | Status | Coverage |
|-------|---------|--------|----------|
| [#2](https://github.com/gardener/etcd-steward/issues/2) | Always-on HTTP server | ✅ Implemented | 85% |
| [#3](https://github.com/gardener/etcd-steward/issues/3) | Leadership observation | ✅ Implemented | 70% |
| [#4](https://github.com/gardener/etcd-steward/issues/4) | etcd lock mechanism | ✅ Complete | 100% |
| [#5](https://github.com/gardener/etcd-steward/issues/5) | Coordinated defragmentation | ⚠️ Partial | 30% |
| [#6](https://github.com/gardener/etcd-steward/issues/6) | Alarm handling | ✅ Implemented | 75% |
| [#7](https://github.com/gardener/etcd-steward/issues/7) | Snapstore (cloud providers) | ⚠️ Partial | 35% |
| [#8](https://github.com/gardener/etcd-steward/issues/8) | Snapshot compression | ✅ Implemented | 80% |
| [#9](https://github.com/gardener/etcd-steward/issues/9) | Snapshotting | ✅ Implemented | 90% |
| [#10](https://github.com/gardener/etcd-steward/issues/10) | Snapshot garbage collection | ⚠️ Partial | 60% |
| [#11](https://github.com/gardener/etcd-steward/issues/11) | Failure-tolerant restoration | ⚠️ Partial | 35% |
| [#12](https://github.com/gardener/etcd-steward/issues/12) | Snapshot compaction subcommand | ⚠️ Partial | 25% |
| [#13](https://github.com/gardener/etcd-steward/issues/13) | Data validation | ⚠️ Partial | 40% |
| [#14](https://github.com/gardener/etcd-steward/issues/14) | etcd bootstrapping | ✅ Implemented | 80% |
| [#15](https://github.com/gardener/etcd-steward/issues/15) | EtcdMember status updation | ✅ Implemented | 70% |
| [#16](https://github.com/gardener/etcd-steward/issues/16) | Member lease heartbeat | ✅ Complete | 100% |

---

## Per-issue detail

### #2 — Always-on HTTP server

**Complete:** All 8 HTTP endpoints implemented and tested. TLS support via `--server-cert`/`--server-key`. Server starts before initialization so etcd-wrapper can connect immediately.

**Gaps:**
- No HSTS or security headers (X-Content-Type-Options, X-Frame-Options) when TLS is enabled
- Component handler registration is via constructor injection, not a dynamic `Register(path, handler)` interface

---

### #3 — Observe etcd cluster leadership changes

**Complete:** `pkg/leaderwatch` polls the etcd `Status` API and detects role changes. `IsLeader()` is used by snapshotter and GC as a gate. Role changes are recorded to `EtcdMember.status.transitions`.

**Gaps:**
- Issue describes a pub-sub model where leadership changes are pushed to all subscribers via non-buffered channels. Current implementation is pull-based: consumers call `IsLeader()` on their own tick.
- Issue prefers option 1 (watch on an etcd key written by etcd-wrapper); option 2 (polling) was implemented instead.

---

### #4 — etcd lock mechanism

**Complete.** `pkg/lock` implements the upstream etcd lock pattern exactly: `LeaseGrant` + atomic `Txn(createRevision==0)` + `Watch` on contention. `Acquire`/`Release` are context-cancellable. Fully tested.

---

### #5 — Coordinated defragmentation

**Complete:** The etcd lock (`pkg/lock`) is available as the coordination primitive. NOSPACE alarm handler bypasses the lock for emergency defragmentation (correct per the issue).

**Gaps:**
- No scheduled defragmentation component. `DefragSchedule` config field is parsed but unused.
- No DB-size threshold check to trigger defrag automatically.
- No multi-member turn-taking coordination (member 0 defrags, signals done, member 1 proceeds, etc.).

---

### #6 — Alarm handling

**Complete:** `pkg/alarm` polls for alarms at a configurable interval. NOSPACE triggers compact + defrag + disarm in sequence. CORRUPT is logged.

**Gaps:**
- CORRUPT alarm is not cross-referenced with the validator during initialization.
- Continuous CORRUPT recovery during etcd operation (delete DB + restore/sync with leader) is not implemented.

---

### #7 — Snapstore for object storage

**Complete:** Clean `Snapstore` interface with `Save`/`Fetch`/`List`/`Delete`. `LocalSnapstore` fully implemented and tested. Snapshot naming convention is consistent and parseable.

**Gaps:**
- S3, GCS, ABS providers return `"not yet implemented"` errors.
- No `CompressedSnapstore` decorator — compression is applied at the snapshotter level instead.

---

### #8 — Snapshot compression

**Complete:** `pkg/compression` provides a clean `Compressor` interface. `gzip`, `zstd`, and `noop` compressors are implemented. zstd is the default (better than zlib from etcd-backup-restore).

**Gaps:**
- `lzw` and `zlib` from etcd-backup-restore are not implemented. These are intentionally superseded by zstd.
- `CompressedSnapstore` wrapper is not implemented — compression is a pipeline step in the snapshotter, not a snapstore decorator.

---

### #9 — Snapshotting

**Complete:** Full snapshot cron schedule, delta snapshot period + size-based flush, leader-only gate, `isFinal` flag for control-plane migration. `POST /snapshot/full` and `POST /snapshot/delta` endpoints are wired.

**Gaps:**
- Delta snapshot replay during restoration is stubbed (`TODO: implement delta replay`).

---

### #10 — Snapshot garbage collection

**Complete:** `pkg/gc` groups snapshots into sets and retains the last N by count (`LimitBased` policy). Leader-only. Orphan incrementals (before the first full snapshot) are handled.

**Gaps:**
- Time-based retention policy not implemented. The issue explicitly requires it as an alternative to count-based.
- `Exponential` policy is referenced in config but not implemented in `gc.go`.

---

### #11 — Failure-tolerant restoration

**Complete:** Full snapshot download, decompress, and placement in the data directory. Cross-filesystem move handled via `copyFile`.

**Gaps:**
- Delta snapshot replay not implemented — restoration uses only the latest full snapshot.
- No checkpoint/resume across pod restarts. If the pod restarts mid-restoration, the process starts over.
- No local snapshot caching to avoid re-downloading on restart.
- No callback to etcd-wrapper to start an embedded etcd for delta replay. The issue specifies this to keep steward memory footprint minimal.

---

### #12 — Snapshot compaction subcommand

**Complete:** `etcd-steward compact` subcommand connects to etcd and issues a history compaction to a given revision.

**Gaps:**
- Full compaction pipeline not implemented. The issue requires: restore snapshot set → compact → defrag → upload new full snapshot. Currently only the raw `Compact` call is present.
- Does not reuse `pkg/restoration` and `pkg/snapshotter`.

---

### #13 — Data validation

**Complete:** `pkg/validator` detects corruption via bbolt `tx.Check()` (Full mode) or DB open (Sanity mode). Exit marker determines which mode runs on next startup.

**Gaps:**
- No revision check (compare DB revision vs latest snapshot revision in store).
- Multi-node etcds: revision checks should be skipped — not yet implemented.
- Single-node: if DB revision lags behind the store, partial restoration should be triggered instead of full — not implemented.
- No volume mismatch detection.
- No structured/typed error codes — errors are plain strings.

---

### #14 — Simple etcd bootstrapping

**Complete:** `pkg/initializer` implements the full DEP-04 lifecycle with paths A–D. TCP pre-check avoids gRPC hang when peers are unreachable. Learner join + promotion is implemented. State transitions are persisted synchronously before each step.

**Gaps:**
- Partial restoration (Path D with delta replay) deferred to #11.
- Multi-node revision validation specifics deferred to #13.

---

### #15 — EtcdMember status updation

**Complete:** State transitions are written synchronously to `EtcdMember.status.transitions` via K8s patch before control flow advances. `EtcdMember.status` is owned exclusively by etcd-steward.

**Gaps:**
- Async status fields are not populated: `lastFullSnapshot`, `lastDeltaSnapshot`, `lastDefragmentation` timestamps are not written back after operations complete.
- No async update pipeline from snapshotter/alarm handler back to EtcdMember status.

---

### #16 — Member lease heartbeat

**Complete.** `pkg/lease` renews the K8s `Lease` resource at `k8s-heartbeat-duration` intervals (default 10s). TTL = 3× interval. Holder identity encodes `{memberID}:{clusterID}:{role}`. Creates lease if missing.

---

## Known limitations (v0.1.0 alpha)

1. **Cloud storage not supported.** S3, GCS, ABS snapstore providers are not implemented. Only the `Local` filesystem provider works.

2. **Delta snapshot replay not implemented.** Restoration uses only the latest full snapshot. Delta snapshots are uploaded but not applied during restoration.

3. **No scheduled defragmentation.** Defragmentation only occurs as part of NOSPACE alarm remediation, not on a schedule or DB-size threshold.

4. **Compaction subcommand is minimal.** The `compact` subcommand performs a raw history compaction only — it does not run the full restore → compact → defrag → upload pipeline.

5. **No time-based GC policy.** Only count-based snapshot retention is implemented.

6. **Restoration is not resumable.** A pod restart during restoration causes restoration to start over from scratch.
