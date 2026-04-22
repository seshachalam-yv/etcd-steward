# Load Test Tool

A standalone etcd load generator for verifying etcd-steward snapshot and GC
behaviour under sustained write pressure.

## Prerequisites

- A running etcd instance (e.g. inside a KIND cluster)
- `kubectl port-forward` to expose the etcd client endpoint

## Build

```bash
cd test/load
go build -o loadtest .
```

## Usage

```bash
# Default: 100 writes/s for 1 minute against localhost:2379
./loadtest

# Custom settings
./loadtest \
  -endpoint http://localhost:2379 \
  -rate 500 \
  -duration 5m \
  -prefix "bench/" \
  -value-size 1024
```

## Running against a KIND cluster

```bash
# 1. Set up a KIND cluster with etcd-steward (see hack/ scripts).

# 2. Port-forward to the etcd pod:
kubectl port-forward -n <namespace> pod/<etcd-pod> 2379:2379

# 3. Run the load test:
./loadtest -endpoint http://localhost:2379 -rate 200 -duration 2m

# 4. Observe steward behaviour:
kubectl logs -n <namespace> <etcd-pod> -c steward -f
```

## Output

The tool prints a summary at the end:

```
=== Load Test Results ===
Duration:     1m0.001s
Total writes: 5987
Total errors: 13
Throughput:   99.8 writes/s
Latency p50:  1.2ms
Latency p95:  3.8ms
Latency p99:  12.1ms
```

## Flags

| Flag          | Default                  | Description                     |
|---------------|--------------------------|---------------------------------|
| `-endpoint`   | `http://localhost:2379`  | etcd client endpoint            |
| `-rate`       | `100`                    | Target writes per second        |
| `-duration`   | `1m`                     | Test duration                   |
| `-prefix`     | `loadtest/`              | Key prefix for generated keys   |
| `-value-size` | `256`                    | Size of each value in bytes     |
