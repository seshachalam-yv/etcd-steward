// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package compression

import (
	"compress/gzip"
	"io"
)

// gzipCompressor implements Compressor using stdlib compress/gzip.
type gzipCompressor struct{}

func (g *gzipCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriter(w), nil
}

func (g *gzipCompressor) Decompress(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}

func (g *gzipCompressor) FileExtension() string { return ".gz" }
