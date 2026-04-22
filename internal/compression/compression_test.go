// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package compression

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestGzipRoundTrip(t *testing.T) {
	original := "hello, gzip compression round-trip test data"
	compressed, err := Compress(strings.NewReader(original), AlgorithmGzip)
	if err != nil {
		t.Fatalf("Compress(gzip) failed: %v", err)
	}

	rc, err := Decompress(compressed, AlgorithmGzip)
	if err != nil {
		t.Fatalf("Decompress(gzip) failed: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(got) != original {
		t.Fatalf("gzip round-trip: got %q, want %q", got, original)
	}
}

func TestZstdRoundTrip(t *testing.T) {
	original := "hello, zstd compression round-trip test data"
	compressed, err := Compress(strings.NewReader(original), AlgorithmZstd)
	if err != nil {
		t.Fatalf("Compress(zstd) failed: %v", err)
	}

	rc, err := Decompress(compressed, AlgorithmZstd)
	if err != nil {
		t.Fatalf("Decompress(zstd) failed: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(got) != original {
		t.Fatalf("zstd round-trip: got %q, want %q", got, original)
	}
}

func TestNoneRoundTrip(t *testing.T) {
	original := "hello, no compression pass-through"
	compressed, err := Compress(strings.NewReader(original), AlgorithmNone)
	if err != nil {
		t.Fatalf("Compress(none) failed: %v", err)
	}

	rc, err := Decompress(compressed, AlgorithmNone)
	if err != nil {
		t.Fatalf("Decompress(none) failed: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(got) != original {
		t.Fatalf("none round-trip: got %q, want %q", got, original)
	}
}

func TestCompress_InvalidAlgorithm(t *testing.T) {
	_, err := Compress(bytes.NewReader(nil), Algorithm("lz4"))
	if err == nil {
		t.Fatal("expected error for invalid algorithm, got nil")
	}
	if !strings.Contains(err.Error(), "unknown compression algorithm") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestDecompress_InvalidAlgorithm(t *testing.T) {
	_, err := Decompress(bytes.NewReader(nil), Algorithm("lz4"))
	if err == nil {
		t.Fatal("expected error for invalid algorithm, got nil")
	}
	if !strings.Contains(err.Error(), "unknown compression algorithm") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
