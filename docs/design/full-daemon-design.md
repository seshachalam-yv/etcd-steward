# etcd-steward Full Daemon Design — Matching etcd-backup-restore Capabilities

## Current State (What's Broken)

The steward's `main.go` was simplified during Session 2 (cloud provider agent overwrote the full wiring).
The current daemon:
- Starts HTTP server with /healthz, /metrics, /initialization/status (plain text), /config
- Returns "Successful" immediately (no actual initialization)
- Returns hardcoded minimal etcd config from /config
- Does NOT start snapshotter, restorer, defrag, GC, or any other component
- Does NOT read the druid-mounted ConfigMap at /var/etcd/config/etcd.conf.yaml

This means:
- **No snapshots are taken** → no backup exists → restore is impossible
- **No data validation** → corruption not detected
- **No restoration** → empty etcd after PVC loss
- **No defragmentation** → DB grows unbounded
- **No GC** → snapshots accumulate forever
- **e2e tests pass falsely** — they only check pod Ready status, not data integrity

## Target State (What It Must Do)

### The Initialization Dance (wrapper ↔ steward)

```
WRAPPER                              STEWARD
  |                                    |
  |  (1) GET /initialization/status -->| returns "New"
  |                                    |
  |  (2) GET /initialization/start  -->| triggers initialization:
  |      ?mode=Full|Sanity             |   a) validate data dir (sanity or full)
  |                                    |   b) if corrupt/empty + single-node:
  |                                    |      - download full snapshot from store
  |                                    |      - restore via etcdutl
  |                                    |   c) if corrupt + multi-node:
  |                                    |      - remove self from cluster
  |                                    |      - clean data dir
  |                                    |   d) if clean + non-empty:
  |                                    |      - nothing (use existing data)
  |                                    |
  |  (3) GET /initialization/status -->| returns "Progress" (during init)
  |  ...polls every 1s...              |
  |  (4) GET /initialization/status -->| returns "Successful" (init done)
  |                                    |
  |  (5) GET /config ----------------->| returns etcd config YAML
  |                                    |  (from mounted ConfigMap or generated)
  |                                    |
  |  (6) wrapper starts embedded etcd  |
  |      using the config              |
  |                                    |
  |  (7) wrapper polls etcd readiness  |
  |      (/readyz returns 200)         |
  |                                    |
  |  === ETCD IS READY ===             |
  |                                    |
  |                              (8) steward connects to etcd client
  |                              (9) starts snapshotter (leader only)
  |                              (10) starts defrag scheduler
  |                              (11) starts GC
  |                              (12) starts alarm handler
  |                              (13) starts member lease renewal
  |                              (14) starts leader watcher
  |                              (15) starts EtcdMember updater
```

### Key Difference from Current Implementation

The initialization is **asynchronous** and **stateful**:
1. Wrapper calls GET /initialization/start → steward starts init in a goroutine
2. Wrapper polls GET /initialization/status → returns "Progress" during init, "Successful" when done
3. Only AFTER "Successful" does wrapper fetch /config and start etcd
4. Only AFTER etcd is running does steward start its components

Current (broken): steward returns "Successful" immediately without doing anything.

### The /config Endpoint

Must return the etcd config YAML that the wrapper will use to start embedded etcd.
Two sources:
1. **Druid-mounted ConfigMap** at `/var/etcd/config/etcd.conf.yaml` — preferred
2. **Generated from steward config flags** — fallback

The wrapper writes this to a file and passes to `embed.ConfigFromFile`.

### Components That Must Run

| Component | When | Leader Only? | What it does |
|-----------|------|-------------|--------------|
| HTTP server | Always | No | /metrics, /healthz, /initialization/*, /config, /snapshot/* |
| Initializer | On /initialization/start | No | Data validation → restore → config generation |
| Snapshotter | After etcd ready | **Yes** | Full + delta snapshots to backup store |
| GC | After etcd ready | **Yes** | Delete old snapshot sets |
| Defragmenter | After etcd ready | **Yes** (coordinates) | Leader-orchestrated defrag |
| Alarm handler | After etcd ready | No | NOSPACE: compact+defrag+disarm |
| Leader watcher | After etcd ready | No | Watch /steward/leader key |
| Member lease | After etcd ready | No | Renew K8s coordination lease |
| EtcdMember updater | After etcd ready | No | Patch EtcdMember status |

### Snapshot Endpoints (matching backup-restore)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| /snapshot/full | POST | Trigger out-of-schedule full snapshot |
| /snapshot/delta | POST | Trigger out-of-schedule delta snapshot |
| /snapshot/latest | GET | Return latest snapshot metadata |

### Config Flow

Steward reads config from:
1. CLI flags (highest priority)
2. Config file (--config flag, YAML)
3. Druid-mounted ConfigMap (for etcd-specific config)
4. Defaults

The druid StatefulSet builder passes steward flags like:
```
--pod-name=$(POD_NAME)
--pod-namespace=$(POD_NAMESPACE)  
--data-dir=/var/etcd/data/new.etcd
--etcd-endpoints=http://test-local:2379
--enable-snapshotter=true
--full-snapshot-interval=24h
--delta-snapshot-interval=20s
--store-provider=Local
--store-prefix=/var/etcd/data/snapshots
```

## Implementation Plan

### Phase 1: Full daemon main.go rewrite

Replace cmd/etcdsteward/main.go with proper daemon wiring:

1. Parse config from flags + file
2. Start HTTP server with ALL endpoints
3. Register initialization handler (async, stateful: New→Progress→Successful)
4. Register /config handler (reads mounted ConfigMap or generates)
5. Wait for initialization to complete
6. Connect etcd client
7. Start all gated components

### Phase 2: Proper initialization handler

The /initialization/start handler must:
1. Set status to "Progress"
2. Run data validation (sanity or full based on mode param)
3. If data corrupt or empty:
   - Single-node: restore from backup store (full + deltas)
   - Multi-node: remove self, clean data, rejoin as learner
4. If data valid: do nothing
5. Set status to "Successful" (or "Failed")

### Phase 3: Proper /config handler

Read the druid-mounted ConfigMap at `/var/etcd/config/etcd.conf.yaml`.
If not mounted, generate config from steward's own flags.
Return as YAML that embed.ConfigFromFile can parse.

### Phase 4: Leader-gated snapshotter

Only the leader takes snapshots. Gate on:
- Etcd ready (initialization complete + wrapper started etcd)
- This member is the leader (from leader watcher)
- Backup store is configured

### Phase 5: E2e verification — real restore test

1. Create Etcd with backup store
2. Write keys
3. Wait for steward to take full snapshot
4. Write more keys → steward takes delta
5. Delete PVC + delete pod
6. Pod restarts → steward detects empty data dir
7. Steward restores from full snapshot
8. Wrapper starts etcd from restored data
9. Steward applies delta snapshots
10. Verify ALL keys are present
