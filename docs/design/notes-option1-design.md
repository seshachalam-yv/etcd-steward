# etcd-steward Daemon Design — Notes Option-1

## Architecture: Steward Orchestrates, Wrapper Executes

Per the design notes (Option-1 Preferred):
- **Steward** is the orchestrator: validates data, restores, tells wrapper to start etcd, applies deltas, controls readiness
- **Wrapper** is the executor: starts embedded etcd when told, exposes /readyz

## Flow (per the notes)

```
STEWARD                                  WRAPPER
  |                                        |
  | (pod starts, both containers start)    | (pod starts)
  |                                        |
  | Step 1: Start HTTP server              | Start HTTP server (/readyz, /stop,
  |   /metrics, /healthz                   |   /embedded-etcd, /readyz/set)
  |                                        |
  | Step 2: Validate data directory        | (idle, waiting for POST /embedded-etcd)
  |   - SanityCheck or FullCheck           |
  |   - Record EtcdMember transition: New  |
  |                                        |
  | Step 3: If data corrupt:               |
  |   - removeMember() [multi-node]        |
  |   - deleteDataDir()                    |
  |                                        |
  | Step 4: If data empty (fresh or after  |
  |         corruption cleanup):           |
  |   IF backup store configured:          |
  |     a) Download latest full snapshot   |
  |     b) Restore via etcdutl to dataDir  |
  |     c) Record transition: Initializing |
  |   ELSE:                                |
  |     (etcd will start with empty data)  |
  |                                        |
  | Step 5: Build etcd config YAML         |
  |   (from mounted ConfigMap or flags)    |
  |                                        |
  | Step 6: POST /embedded-etcd ---------->| receives config
  |   body: etcd config YAML              |  writes to file
  |                                        |  starts embed.Etcd(cfg)
  |                                        |  returns 202 Accepted
  |                                        |
  | Step 7: Wait for etcd to be ready     |
  |   (poll etcd client Get until success) |
  |                                        |
  | Step 8: If restoration happened:       |
  |   - Connect etcd client               |
  |   - Apply delta snapshots via KV      |
  |   - Record transition: Started        |
  |                                        |
  | Step 9: POST /readyz/set "ready" ---->| /readyz now returns 200
  |                                        | K8s readiness probe passes
  |                                        |
  | === ETCD IS FULLY READY ===           |
  |                                        |
  | Step 10: Start runtime components:     |
  |   - Leader watcher                    |
  |   - Snapshotter (leader only)         |
  |   - GC (leader only)                  |
  |   - Defragmenter (leader-coordinated) |
  |   - Alarm handler                     |
  |   - Member lease renewer              |
  |   - EtcdMember updater               |
  |                                        |
  | Wrapper pushes leadership changes:     |
  |   writes /steward/leader key <--------| on leadership change
```

## Readiness (from notes)

> The readiness endpoint will only publish ready status when:
> 1. In case of restoration: steward has successfully applied all delta snapshots
> 2. In case of scale-up/restart: learner gets promoted to a voting member

This means: steward controls when the pod is "ready" via POST /readyz/set.
The wrapper's /readyz returns 503 until steward says "ready".
K8s readiness probe points to wrapper's /readyz.

## Wrapper Endpoints Required (from notes)

| Endpoint | Verb | Purpose |
|----------|------|---------|
| POST /embedded-etcd | POST | Steward sends etcd config, wrapper starts embedded etcd |
| POST /readyz/set | POST | Steward controls readiness (body: "ready" or "unready") |
| GET /readyz | GET | K8s readiness probe (returns 200 only after steward says ready) |
| GET /stop | POST | Shutdown embedded etcd |

These are already implemented in the wrapper worktree (feat/steward-compat).

## Steward HTTP Endpoints

| Endpoint | Verb | Purpose |
|----------|------|---------|
| GET /metrics | GET | Prometheus metrics (always on) |
| GET /healthz | GET | Liveness probe (always returns 200) |
| POST /snapshot/full | POST | Trigger out-of-schedule full snapshot |
| POST /snapshot/delta | POST | Trigger out-of-schedule delta snapshot |
| GET /snapshot/latest | GET | Latest snapshot metadata |

NOTE: No /initialization/* endpoints needed in the notes' architecture!
The steward drives the flow, not the wrapper. The /initialization/* endpoints
were only needed for backward compatibility with the existing wrapper.

## Druid StatefulSet Changes

The druid builder (when UseEtcdSteward is enabled) must:
1. Set etcd container readiness probe to wrapper's /readyz (port 9095) — NOT steward's /healthz
2. The wrapper's /readyz now returns 503 by default (steward controls it)
3. Steward container doesn't need a readiness probe (it's always running)

## Container Args

### Wrapper container (etcd):
```
start-etcd
--steward-host-port=localhost:8080    # NEW: steward's HTTP port
--steward-tls-enabled=false           # NEW: TLS to steward
```

### Steward container (backup-restore):
```
--pod-name=$(POD_NAME)
--pod-namespace=$(POD_NAMESPACE)
--server-port=8080
--data-dir=/var/etcd/data/new.etcd
--etcd-endpoints=http://test-local:2379
--wrapper-url=http://localhost:9095    # NEW: wrapper's HTTP port
--enable-snapshotter=true
--store-provider=Local
--store-prefix=...
```
