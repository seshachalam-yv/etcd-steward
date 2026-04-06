// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package compression

import (
	"bytes"
	"io"
	"testing"
)

var testPayload = bytes.Repeat([]byte("etcd-steward snapshot compression test payload "), 200)

func TestNewCompressor_UnknownPolicy(t *testing.T) {
	_, err := NewCompressor("bogus")
	if err == nil {
		t.Fatal("expected error for unknown policy, got nil")
	}
}

func TestNewCompressor_EmptyIsSameAsNone(t *testing.T) {
	c, err := NewCompressor("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ext := c.FileExtension(); ext != "" {
		t.Errorf("expected empty extension, got %q", ext)
	}
}

func roundTrip(t *testing.T, policy string) {
	t.Helper()
	c, err := NewCompressor(policy)
	if err != nil {
		t.Fatalf("NewCompressor(%q) error: %v", policy, err)
	}

	// Compress
	var compressed bytes.Buffer
	wc, err := c.Compress(&compressed)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}
	if _, err := wc.Write(testPayload); err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if err := wc.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	if compressed.Len() == 0 {
		t.Fatal("compressed output is empty")
	}

	// Decompress
	rc, err := c.Decompress(&compressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}
	decompressed, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Decompress Close error: %v", err)
	}

	if !bytes.Equal(decompressed, testPayload) {
		t.Errorf("round-trip mismatch: got %d bytes, want %d bytes", len(decompressed), len(testPayload))
	}
}

func TestNone_RoundTrip(t *testing.T) {
	roundTrip(t, "none")
}

func TestGzip_RoundTrip(t *testing.T) {
	roundTrip(t, "gzip")
}

func TestZstd_RoundTrip(t *testing.T) {
	roundTrip(t, "zstd")
}

func TestGzip_DecompressInvalidInput(t *testing.T) {
	c, _ := NewCompressor("gzip")
	_, err := c.Decompress(bytes.NewReader([]byte("not-gzip-data")))
	if err == nil {
		t.Fatal("expected error decompressing invalid gzip data")
	}
}

func TestZstd_DecompressInvalidInput(t *testing.T) {
	c, _ := NewCompressor("zstd")
	rc, err := c.Decompress(bytes.NewReader([]byte("not-zstd-data")))
	if err != nil {
		// Error on reader creation is also acceptable
		return
	}
	_, err = io.ReadAll(rc)
	if err == nil {
		t.Fatal("expected error decompressing invalid zstd data")
	}
}
