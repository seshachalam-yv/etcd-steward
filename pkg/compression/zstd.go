// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package compression

import (
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// zstdCompressor implements Compressor using klauspost/compress/zstd.
type zstdCompressor struct {
	encoder *zstd.Encoder
}

func newZstdCompressor() (*zstdCompressor, error) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd encoder: %w", err)
	}
	return &zstdCompressor{encoder: enc}, nil
}

func (z *zstdCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	enc, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("zstd: failed to create writer: %w", err)
	}
	return enc, nil
}

func (z *zstdCompressor) Decompress(r io.Reader) (io.ReadCloser, error) {
	dec, err := zstd.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("zstd: failed to create reader: %w", err)
	}
	return dec.IOReadCloser(), nil
}

func (z *zstdCompressor) FileExtension() string { return ".zst" }
