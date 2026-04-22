// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package compression

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Algorithm represents a compression algorithm.
type Algorithm string

const (
	// AlgorithmGzip uses gzip compression.
	AlgorithmGzip Algorithm = "gzip"
	// AlgorithmZstd uses zstandard compression.
	AlgorithmZstd Algorithm = "zstd"
	// AlgorithmNone performs no compression (pass-through).
	AlgorithmNone Algorithm = "none"
)

// Compress reads from src and returns a reader of the compressed data using the
// specified algorithm. For AlgorithmNone the source reader is returned as-is.
func Compress(src io.Reader, algo Algorithm) (io.Reader, error) {
	switch algo {
	case AlgorithmGzip:
		return compressGzip(src)
	case AlgorithmZstd:
		return compressZstd(src)
	case AlgorithmNone:
		return src, nil
	default:
		return nil, fmt.Errorf("unknown compression algorithm: %q", algo)
	}
}

// Decompress returns a ReadCloser that decompresses data read from src using
// the specified algorithm. The caller must close the returned ReadCloser.
// For AlgorithmNone the source reader is wrapped in a no-op closer.
func Decompress(src io.Reader, algo Algorithm) (io.ReadCloser, error) {
	switch algo {
	case AlgorithmGzip:
		return gzip.NewReader(src)
	case AlgorithmZstd:
		return decompressZstd(src)
	case AlgorithmNone:
		return io.NopCloser(src), nil
	default:
		return nil, fmt.Errorf("unknown compression algorithm: %q", algo)
	}
}

func compressGzip(src io.Reader) (io.Reader, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := io.Copy(w, src); err != nil {
		return nil, fmt.Errorf("gzip compress: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("gzip close: %w", err)
	}
	return &buf, nil
}

func compressZstd(src io.Reader) (io.Reader, error) {
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	if err != nil {
		return nil, fmt.Errorf("zstd writer: %w", err)
	}
	if _, err := io.Copy(w, src); err != nil {
		return nil, fmt.Errorf("zstd compress: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("zstd close: %w", err)
	}
	return &buf, nil
}

func decompressZstd(src io.Reader) (io.ReadCloser, error) {
	r, err := zstd.NewReader(src)
	if err != nil {
		return nil, fmt.Errorf("zstd reader: %w", err)
	}
	return io.NopCloser(r), nil
}
