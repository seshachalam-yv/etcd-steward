<!--
SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Design Decisions

This document records the key design decisions made for etcd-steward, the rationale behind each choice, and the alternatives that were considered.

## Defragmentation: Leader-Coordinated (Option #3)

**Decision**: The cluster leader orchestrates defragmentation across all members. Followers are defragged first, the leader last.

**Why**: This approach requires no external dependency (no separate controller or CRD). The leader already has a view of cluster membership via the etcd cluster API. By defragging followers first, the cluster maintains quorum throughout the process. A distributed lock (etcd concurrency mutex) ensures only one member is defragged at a time.

**Alternatives considered**:

- *Option #1: Each member defrags itself independently*. Rejected because concurrent defrag on multiple members risks temporary quorum loss.
- *Option #2: External controller triggers defrag*. Rejected because it introduces an external dependency and complicates the operational model.

**Implementation**: `internal/defrag` -- the `Defragmenter` acquires a distributed lock per member, sorts endpoints so the leader is last, and writes status keys to etcd for observability. The leader skips cycles when it is not the leader.

## Initialization: Steward Validates, Wrapper Starts etcd (Option #1)

**Decision**: etcd-steward owns the initialization logic (data directory validation, snapshot restore, cluster membership management) and delegates the actual etcd process start to the etcd-wrapper sidecar via its HTTP API.

**Why**: This keeps the resource footprint low -- there is no need for a separate init container or a heavyweight initialization process. The steward already has access to the snapstore and the etcd client, so it can make informed decisions about whether to restore, join as a learner, or start fresh. The wrapper's only responsibility is managing the embedded etcd process lifecycle.

**Alternatives considered**:

- *Option #2: Separate init container*. Rejected because it would duplicate snapstore and etcd client logic, and init containers cannot communicate with the running pod's sidecars.
- *Option #3: Wrapper handles everything*. Rejected because it would make the wrapper significantly more complex and couple it to snapstore/restore logic.

**Implementation**: `internal/bootstrapper` -- the `Bootstrapper` checks data directory state, handles corruption recovery, learner join flow, snapshot restore, and delegates `StartEmbeddedEtcd` / `CheckReady` to the wrapper client interface.

## Leadership Detection: Watch Key in etcd (Option #1)

**Decision**: Leadership is determined by polling a well-known etcd key (`/steward/leader`). The current leader writes its pod name to this key.

**Why**: This leverages etcd's own watch and consistency guarantees. No additional infrastructure (e.g. Kubernetes Lease-based leader election) is needed for determining which steward instance is the leader. The approach is simple, reliable, and does not introduce a dependency on the Kubernetes API server for leader election.

**Alternatives considered**:

- *Option #2: Kubernetes Lease-based leader election*. Rejected because it adds a dependency on the Kubernetes API server, which may be unavailable during etcd outages (circular dependency).
- *Option #3: Raft leader detection via etcd status*. Rejected because the etcd maintenance Status API returns the raft leader ID, not a pod name, requiring additional mapping logic.

**Implementation**: `internal/leaderwatch` -- the `Watcher` polls the leader key at a configurable period, updates the current role (Leader/Follower/Unknown), and notifies subscribers via buffered channels on role changes.

## Snapshotter: etcd Lease Mutual Exclusion

**Decision**: The snapshotter acquires an etcd distributed lock (concurrency mutex) before taking snapshots. Only one steward instance takes snapshots at a time.

**Why**: This prevents duplicate snapshots when multiple steward instances are running (one per etcd member). The etcd concurrency package provides a well-tested distributed lock primitive backed by etcd leases. If the lock holder crashes, the lease expires and another instance can take over.

**Alternatives considered**:

- *Kubernetes Lease-based locking*. Rejected for the same reason as leadership detection -- circular dependency during etcd outages.
- *Leader-only snapshotting*. Rejected because the snapshotter needs to run continuously, and leader changes should not cause snapshot gaps.

**Implementation**: `internal/snapshotter` -- the `Snapshotter.Run()` method acquires the lock before starting the snapshot loop and releases it on context cancellation. The lock uses `internal/etcdclient.Lock` which wraps `concurrency.NewSession` + `concurrency.NewMutex`.

## Garbage Collection: Snapshot-Set Based

**Decision**: GC operates on snapshot sets, not individual snapshot files. A snapshot set is a full snapshot plus all subsequent delta snapshots up to (but not including) the next full snapshot.

**Why**: Deleting a full snapshot without its deltas (or vice versa) leaves the restore chain broken. By treating the full + deltas as an atomic unit, GC ensures that any retained snapshot set is independently restorable. This also simplifies retention policies -- counting "how many sets to keep" is more intuitive than counting individual files.

**Alternatives considered**:

- *File-based GC*. Rejected because it risks leaving orphaned deltas or breaking restore chains.
- *Revision-based GC*. Rejected because it would require tracking which revisions are covered by which files, adding complexity without benefit.

**Implementation**: `internal/gc` -- `GroupSnapshots()` partitions a flat list into `SnapshotSet` slices sorted oldest-first. Three retention policies are supported:

- `CountPolicy`: Keep the newest N sets.
- `TimePolicy`: Keep sets created within a duration.
- `CalendarPolicy`: Cascading retention at hourly/daily/weekly/monthly granularity.

The latest set is never deleted regardless of policy.

## No Ginkgo: Native `testing.T`

**Decision**: All tests use Go's standard `testing.T` package. Ginkgo/Gomega are not used.

**Why**: The project notes mandate native `testing.T`. Using the standard library reduces dependencies, avoids the Ginkgo runner overhead, and makes tests accessible to any Go developer without learning a BDD framework. Table-driven tests provide the same multi-scenario coverage that Ginkgo `DescribeTable` offers.

**Conventions**:

- `TestFunctionName_Scenario` naming.
- Table-driven tests with `t.Run()` for multiple cases.
- Mock interfaces defined per test file.
- `zap.NewNop()` for silent test loggers.
- `t.TempDir()` for filesystem isolation.

## NDJSON Event Format

**Decision**: Delta snapshots store etcd mutation events as newline-delimited JSON (NDJSON). Each line is a single JSON object representing one PUT or DELETE event.

**Why**: NDJSON is simple, streamable, and human-readable. It can be processed line-by-line without loading the entire file into memory, which is important for large delta snapshots. Standard JSON tooling (jq, etc.) can inspect individual events. The format is self-describing -- no external schema file is needed.

**Alternatives considered**:

- *Protobuf*. Rejected because it adds a compile-time dependency on proto tooling and makes debugging harder (binary format).
- *Full JSON array*. Rejected because it requires reading the entire file before processing any events, and the closing bracket makes streaming writes fragile.

**Implementation**: `internal/snapshotter/event.go` -- `WriteEvents()` uses `json.NewEncoder` to write one event per line. `ReadEvents()` uses `bufio.Scanner` to read line-by-line and `json.Unmarshal` each line independently.

```json
{"type":"PUT","key":"L2Zvbw==","value":"YmFy","revision":42}
{"type":"DELETE","key":"L2Jheg==","revision":43}
```

Keys and values are base64-encoded (standard JSON byte slice encoding).
