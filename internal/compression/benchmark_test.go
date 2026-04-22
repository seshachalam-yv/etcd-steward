// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package compression

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"strings"
	"testing"
)

// BenchmarkGzipCompress benchmarks gzip compression with repetitive data
// (simulates typical etcd snapshot data with many similar keys).
func BenchmarkGzipCompress(b *testing.B) {
	data := generateCompressibleData(64 * 1024) // 64 KB
	b.ResetTimer()
	for range b.N {
		_, err := Compress(bytes.NewReader(data), AlgorithmGzip)
		if err != nil {
			b.Fatalf("Compress(gzip): %v", err)
		}
	}
}

// BenchmarkZstdCompress benchmarks zstd compression with repetitive data.
func BenchmarkZstdCompress(b *testing.B) {
	data := generateCompressibleData(64 * 1024) // 64 KB
	b.ResetTimer()
	for range b.N {
		_, err := Compress(bytes.NewReader(data), AlgorithmZstd)
		if err != nil {
			b.Fatalf("Compress(zstd): %v", err)
		}
	}
}

// BenchmarkGzipDecompress benchmarks gzip decompression.
func BenchmarkGzipDecompress(b *testing.B) {
	data := generateCompressibleData(64 * 1024)
	compressed, err := Compress(bytes.NewReader(data), AlgorithmGzip)
	if err != nil {
		b.Fatalf("Compress(gzip): %v", err)
	}
	compressedData, err := io.ReadAll(compressed)
	if err != nil {
		b.Fatalf("ReadAll: %v", err)
	}

	b.ResetTimer()
	for range b.N {
		rc, err := Decompress(bytes.NewReader(compressedData), AlgorithmGzip)
		if err != nil {
			b.Fatalf("Decompress(gzip): %v", err)
		}
		if _, err := io.ReadAll(rc); err != nil {
			b.Fatalf("ReadAll: %v", err)
		}
		_ = rc.Close()
	}
}

// BenchmarkZstdDecompress benchmarks zstd decompression.
func BenchmarkZstdDecompress(b *testing.B) {
	data := generateCompressibleData(64 * 1024)
	compressed, err := Compress(bytes.NewReader(data), AlgorithmZstd)
	if err != nil {
		b.Fatalf("Compress(zstd): %v", err)
	}
	compressedData, err := io.ReadAll(compressed)
	if err != nil {
		b.Fatalf("ReadAll: %v", err)
	}

	b.ResetTimer()
	for range b.N {
		rc, err := Decompress(bytes.NewReader(compressedData), AlgorithmZstd)
		if err != nil {
			b.Fatalf("Decompress(zstd): %v", err)
		}
		if _, err := io.ReadAll(rc); err != nil {
			b.Fatalf("ReadAll: %v", err)
		}
		_ = rc.Close()
	}
}

// BenchmarkGzipCompress_LargePayload benchmarks gzip compression with 1 MB data.
func BenchmarkGzipCompress_LargePayload(b *testing.B) {
	data := generateCompressibleData(1024 * 1024) // 1 MB
	b.ResetTimer()
	for range b.N {
		_, err := Compress(bytes.NewReader(data), AlgorithmGzip)
		if err != nil {
			b.Fatalf("Compress(gzip): %v", err)
		}
	}
}

// BenchmarkZstdCompress_LargePayload benchmarks zstd compression with 1 MB data.
func BenchmarkZstdCompress_LargePayload(b *testing.B) {
	data := generateCompressibleData(1024 * 1024) // 1 MB
	b.ResetTimer()
	for range b.N {
		_, err := Compress(bytes.NewReader(data), AlgorithmZstd)
		if err != nil {
			b.Fatalf("Compress(zstd): %v", err)
		}
	}
}

// BenchmarkGzipCompress_RandomData benchmarks gzip compression with
// incompressible random data.
func BenchmarkGzipCompress_RandomData(b *testing.B) {
	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		b.Fatalf("rand.Read: %v", err)
	}
	b.ResetTimer()
	for range b.N {
		_, err := Compress(bytes.NewReader(data), AlgorithmGzip)
		if err != nil {
			b.Fatalf("Compress(gzip): %v", err)
		}
	}
}

// BenchmarkZstdCompress_RandomData benchmarks zstd compression with
// incompressible random data.
func BenchmarkZstdCompress_RandomData(b *testing.B) {
	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		b.Fatalf("rand.Read: %v", err)
	}
	b.ResetTimer()
	for range b.N {
		_, err := Compress(bytes.NewReader(data), AlgorithmZstd)
		if err != nil {
			b.Fatalf("Compress(zstd): %v", err)
		}
	}
}

// BenchmarkNoneCompress benchmarks pass-through (no compression).
func BenchmarkNoneCompress(b *testing.B) {
	data := generateCompressibleData(64 * 1024)
	b.ResetTimer()
	for range b.N {
		_, err := Compress(bytes.NewReader(data), AlgorithmNone)
		if err != nil {
			b.Fatalf("Compress(none): %v", err)
		}
	}
}

// generateCompressibleData produces data that simulates typical etcd snapshot
// content with many similar NDJSON lines. This compresses well with both gzip
// and zstd.
func generateCompressibleData(approxSize int) []byte {
	var buf bytes.Buffer
	line := fmt.Sprintf(`{"type":"PUT","key":"/registry/pods/default/pod-XXXX","value":"%s","revision":12345}`,
		strings.Repeat("abcdefghij", 5))
	lineBytes := []byte(line + "\n")
	for buf.Len() < approxSize {
		buf.Write(lineBytes)
	}
	return buf.Bytes()
}
