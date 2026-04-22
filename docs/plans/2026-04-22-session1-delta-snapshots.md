> **For agentic workers:** Gate 1 is pre-approved. Proceed directly to implementation.

## Issue
Link: https://github.com/gardener/etcd-steward/issues/9
Summary: Implement real event-based delta snapshots and delta restoration in etcd-steward.

## Fork Root
Path: /Users/I568019/go/src/github.com/seshachalam-yv/etcd-steward

## Change Type
[x] Enhancement to existing packages (snapshotter, restorer)

## Design Summary

### Current state
- `TakeDeltaSnapshot` stores a minimal text marker (`delta:100-200`) — not actual etcd events
- `ApplyDeltas` is a stub returning nil

### Chosen approach
Use the etcd Watch API to capture events between revisions, serialize them as JSON, and store via snapstore. During restoration, download delta snapshots, deserialize events, and replay them via KV.Put/KV.Delete on a running etcd.

### Event format
```go
type Event struct {
    Type     EventType `json:"type"`     // PUT or DELETE
    Key      []byte    `json:"key"`
    Value    []byte    `json:"value"`    // nil for DELETE
    Revision int64     `json:"revision"`
}
```
Events are stored as newline-delimited JSON (NDJSON) — one event per line. This is simple, streamable, and human-readable for debugging. Compatible with etcd-backup-restore's event model (same semantic structure).

### Constraints from notes
- Max delta snapshot size: 100MB
- No cap on events per delta snapshot
- Max 1M total events before compaction triggers
- Restoration optimization: only apply revisions after the last revision in etcd-db

## Tasks

- [ ] Task 1: Event type and serialization
      Files: internal/snapshotter/event.go, internal/snapshotter/event_test.go
      Tests: unit
      API generation: no
      Acceptance criteria:
      - `TestEvent_MarshalRoundTrip`: Marshal then Unmarshal a PUT event produces identical Key/Value/Revision
      - `TestEvent_MarshalDelete`: DELETE event serializes with nil Value
      - `TestEvents_NDJSONRoundTrip`: WriteEvents then ReadEvents on 100 events produces same sequence
      - `TestReadEvents_EmptyReader`: empty reader returns empty slice, no error

- [ ] Task 2: Real delta snapshots via Watch API
      Files: internal/snapshotter/snapshotter.go (modify TakeDeltaSnapshot), internal/snapshotter/snapshotter_test.go (add/update tests)
      Tests: unit
      API generation: no
      Acceptance criteria:
      - `TestTakeDeltaSnapshot_CapturesEvents`: mock Watch returns 5 events, uploaded snapshot contains 5 NDJSON lines
      - `TestTakeDeltaSnapshot_SkipsWhenNoNewRevisions`: rev unchanged, no upload, no watch call
      - `TestTakeDeltaSnapshot_RespectsMaxSize`: when accumulated event bytes exceed 100MB threshold, snapshot is finalized early
      - Snapshotter struct gains a `watcher` field (etcd Watch interface) and TakeDeltaSnapshot uses it

- [ ] Task 3: Delta restoration — replay events via KV
      Files: internal/restorer/restorer.go (implement ApplyDeltas), internal/restorer/restorer_test.go (add tests)
      Tests: unit
      API generation: no
      Acceptance criteria:
      - `TestApplyDeltas_ReplaysPutEvents`: 3 PUT events applied, mock KV.Put called 3 times with correct key/value
      - `TestApplyDeltas_ReplaysDeleteEvents`: 2 DELETE events applied, mock KV.Delete called 2 times
      - `TestApplyDeltas_SkipsAlreadyAppliedRevisions`: events with revision <= currentRev are skipped
      - `TestApplyDeltas_MultipleSnapshots`: 3 delta snapshots applied in order, all events replayed
      - ApplyDeltas takes a KV client parameter (not just snapshots) to replay into

- [ ] Task 4: Integration — verify full + delta cycle
      Files: internal/snapshotter/snapshotter_test.go (integration test), internal/restorer/restorer_test.go (integration test)
      Tests: unit (using mocks that simulate the full cycle)
      API generation: no
      Acceptance criteria:
      - `TestFullDeltaCycle`: take full snapshot, write 10 keys, take delta, verify delta contains 10 events via ReadEvents
      - `TestRestoreFullThenApplyDeltas`: full snapshot restored, 2 delta snapshots applied, mock KV received all events in order

## PR Checklist (pre-submission)
- [ ] make build passes
- [ ] make test-unit passes with -race
- [ ] All new tests pass
- [ ] No changes to existing passing tests break
- [ ] Event format documented in code comments

## Rollback
Revert the commit on branch feat/issue-1/etcd-steward-v2.
