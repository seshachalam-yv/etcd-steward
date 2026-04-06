// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

// Package copybackups provides the etcd-steward copy-backups subcommand.
package copybackups

import (
	"fmt"
	"io"

	"go.uber.org/zap"

	"github.com/gardener/etcd-steward/pkg/snapstore"
)

// Options holds the configuration for the copy-backups subcommand.
type Options struct {
	SourceProvider  string
	SourceContainer string
	DestProvider    string
	DestContainer   string
}

// Run executes the copy-backups subcommand.
// It creates source and destination snapstores, lists all snapshots in the source,
// and copies each one to the destination.
func Run(logger *zap.Logger, opts Options) error {
	srcStore, err := snapstore.NewSnapstore(snapstore.SnapstoreConfig{
		Provider:  opts.SourceProvider,
		Container: opts.SourceContainer,
	})
	if err != nil {
		return fmt.Errorf("failed to create source snapstore: %w", err)
	}

	dstStore, err := snapstore.NewSnapstore(snapstore.SnapstoreConfig{
		Provider:  opts.DestProvider,
		Container: opts.DestContainer,
	})
	if err != nil {
		return fmt.Errorf("failed to create destination snapstore: %w", err)
	}

	snaps, err := srcStore.List()
	if err != nil {
		return fmt.Errorf("failed to list source snapshots: %w", err)
	}

	logger.Info("copying snapshots",
		zap.Int("count", len(snaps)),
		zap.String("source", opts.SourceContainer),
		zap.String("destination", opts.DestContainer),
	)

	for _, snap := range snaps {
		reader, err := srcStore.Fetch(snap)
		if err != nil {
			return fmt.Errorf("failed to fetch snapshot %s: %w", snap.Path(), err)
		}

		if err := dstStore.Save(snap, reader); err != nil {
			return fmt.Errorf("failed to save snapshot %s to destination: %w", snap.Path(), err)
		}

		logger.Info("copied snapshot", zap.String("path", snap.Path()))
	}

	logger.Info("copy-backups completed", zap.Int("total", len(snaps)))
	return nil
}

// ParseArgs parses copy-backups subcommand arguments from os.Args.
func ParseArgs(args []string) (Options, error) {
	var opts Options
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "--source-provider" && i+1 < len(args):
			opts.SourceProvider = args[i+1]
			i++
		case args[i] == "--source-container" && i+1 < len(args):
			opts.SourceContainer = args[i+1]
			i++
		case args[i] == "--dest-provider" && i+1 < len(args):
			opts.DestProvider = args[i+1]
			i++
		case args[i] == "--dest-container" && i+1 < len(args):
			opts.DestContainer = args[i+1]
			i++
		}
	}
	return opts, nil
}

// Ensure io is used (interface compliance).
var _ io.Reader
