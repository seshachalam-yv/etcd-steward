// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package main implements a basic load generator that writes keys to etcd at a
// configurable rate. It is used to verify etcd-steward snapshot and GC behaviour
// under sustained write pressure.
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"sync/atomic"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	endpoint := flag.String("endpoint", "http://localhost:2379", "etcd endpoint")
	rate := flag.Int("rate", 100, "target writes per second")
	duration := flag.Duration("duration", 1*time.Minute, "test duration")
	keyPrefix := flag.String("prefix", "loadtest/", "key prefix")
	valueSize := flag.Int("value-size", 256, "value size in bytes")
	flag.Parse()

	if *rate <= 0 {
		fmt.Fprintln(os.Stderr, "rate must be > 0")
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{*endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create etcd client: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = cli.Close() }()

	// Pre-generate a random value payload.
	value := make([]byte, *valueSize)
	if _, err := rand.Read(value); err != nil {
		fmt.Fprintf(os.Stderr, "failed to generate random value: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("load test: endpoint=%s rate=%d/s duration=%s prefix=%s value-size=%d\n",
		*endpoint, *rate, *duration, *keyPrefix, *valueSize)

	var (
		totalWrites atomic.Int64
		totalErrors atomic.Int64
		latencies   = make(chan time.Duration, (*rate)*int(duration.Seconds())+1024)
	)

	deadline := time.After(*duration)
	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()

	start := time.Now()

loop:
	for seq := int64(0); ; seq++ {
		select {
		case <-ctx.Done():
			break loop
		case <-deadline:
			break loop
		case <-ticker.C:
			key := fmt.Sprintf("%s%08d", *keyPrefix, seq)
			t0 := time.Now()
			_, putErr := cli.Put(ctx, key, string(value))
			lat := time.Since(t0)

			if putErr != nil {
				totalErrors.Add(1)
			} else {
				totalWrites.Add(1)
				select {
				case latencies <- lat:
				default:
					// Drop latency sample if channel is full.
				}
			}
		}
	}
	elapsed := time.Since(start)

	close(latencies)
	var lats []time.Duration
	for l := range latencies {
		lats = append(lats, l)
	}

	fmt.Println()
	fmt.Println("=== Load Test Results ===")
	fmt.Printf("Duration:     %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Total writes: %d\n", totalWrites.Load())
	fmt.Printf("Total errors: %d\n", totalErrors.Load())
	if len(lats) > 0 {
		fmt.Printf("Throughput:   %.1f writes/s\n", float64(totalWrites.Load())/elapsed.Seconds())
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		fmt.Printf("Latency p50:  %s\n", percentile(lats, 50))
		fmt.Printf("Latency p95:  %s\n", percentile(lats, 95))
		fmt.Printf("Latency p99:  %s\n", percentile(lats, 99))
	}
}

// percentile returns the p-th percentile of a sorted slice of durations.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
