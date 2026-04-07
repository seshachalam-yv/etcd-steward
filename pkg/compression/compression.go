// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package compression provides pluggable snapshot compression and decompression.
package compression

import (
	"fmt"
	"io"
)

// Compressor compresses and decompresses snapshot byte streams.
type Compressor interface {
	// Compress wraps the given writer with compression. The caller must Close the
	// returned WriteCloser to flush and finalize the compressed stream.
	Compress(w io.Writer) (io.WriteCloser, error)
	// Decompress wraps the given reader with decompression.
	Decompress(r io.Reader) (io.ReadCloser, error)
	// FileExtension returns the file extension for the compression format (e.g. ".gz").
	FileExtension() string
}

// NewCompressor returns a Compressor for the given compression policy.
// Supported values: "gzip", "zstd", "none", "" (same as none).
func NewCompressor(policy string) (Compressor, error) {
	switch policy {
	case "gzip":
		return &gzipCompressor{}, nil
	case "zstd":
		return newZstdCompressor()
	case "none", "":
		return &noopCompressor{}, nil
	default:
		return nil, fmt.Errorf("unsupported compression policy %q: use \"gzip\", \"zstd\", or \"none\"", policy)
	}
}

// noopCompressor is the identity compressor that performs no compression.
type noopCompressor struct{}

func (n *noopCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return &noopWriteCloser{w: w}, nil
}

func (n *noopCompressor) Decompress(r io.Reader) (io.ReadCloser, error) {
	return io.NopCloser(r), nil
}

func (n *noopCompressor) FileExtension() string { return "" }

// noopWriteCloser wraps an io.Writer as an io.WriteCloser where Close is a no-op.
type noopWriteCloser struct {
	w io.Writer
}

func (nwc *noopWriteCloser) Write(p []byte) (int, error) {
	return nwc.w.Write(p)
}

func (nwc *noopWriteCloser) Close() error {
	return nil
}
