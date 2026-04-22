> **For agentic workers:** Gate 1 is pre-approved. Fix immediately.

## Issue
Summary: Restore backward-compatible /initialization/* endpoints in steward daemon while keeping Notes Option-1 POST /embedded-etcd flow.

## Fork Root
Path: /Users/I568019/go/src/github.com/gardener/etcd-steward/.worktrees/etcd-steward-v2

## Change Type
[x] Bug fix — steward broke backward compat with deployed wrapper

## Design Summary
The last commit removed /initialization/status, /initialization/start, and /config endpoints.
The deployed wrapper (v0.6.2) requires these. Restore them as a backward-compat layer:

1. Steward starts → registers ALL endpoints (/initialization/*, /config, /snapshot/*, AND /embedded-etcd flow)
2. Steward validates data dir + restores from backup (same as current code)
3. TWO parallel paths:
   - Path A (old wrapper): steward returns Successful from /initialization/status, wrapper calls /config, starts etcd
   - Path B (new wrapper): steward POST /embedded-etcd to wrapper directly
4. Steward detects which wrapper is running: if POST /embedded-etcd fails (404), fall through to Path A
5. After etcd is reachable, start all components

## Tasks
- [ ] Task 1: Restore /initialization/* and /config endpoints in runDaemon()
      Files: cmd/etcdsteward/main.go
      Acceptance criteria:
      - GET /initialization/status returns plain text "New" initially, "Successful" after init completes
      - GET /initialization/start triggers init (async), returns 200
      - GET /config returns etcd config YAML from mounted ConfigMap or generated
      - POST /embedded-etcd still works for new wrapper

## Rollback
Revert the commit.
