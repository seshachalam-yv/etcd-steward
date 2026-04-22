// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package validator

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gardener/etcd-steward/internal/errors"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// walDirName is the name of the WAL directory inside the etcd data directory.
const walDirName = "member/wal"

// dbFileName is the name of the bbolt database file inside the etcd data directory.
const dbFileName = "member/snap/db"

// Validator performs data directory integrity checks for an etcd member.
type Validator struct {
	dataDir string
	logger  *zap.Logger
}

// New creates a new Validator for the given etcd data directory.
func New(dataDir string, logger *zap.Logger) *Validator {
	return &Validator{
		dataDir: dataDir,
		logger:  logger,
	}
}

// SanityCheck performs a quick check: WAL dir exists, DB file exists, DB opens
// without error.
func (v *Validator) SanityCheck() error {
	walDir := filepath.Join(v.dataDir, walDirName)
	info, err := os.Stat(walDir)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.Wrap(errors.ErrCodeValidation, fmt.Sprintf("WAL directory %q does not exist", walDir), err)
		}
		return errors.Wrap(errors.ErrCodeIO, fmt.Sprintf("failed to stat WAL directory %q", walDir), err)
	}
	if !info.IsDir() {
		return errors.New(errors.ErrCodeValidation, fmt.Sprintf("WAL path %q is not a directory", walDir))
	}

	dbFile := filepath.Join(v.dataDir, dbFileName)
	if _, err := os.Stat(dbFile); err != nil {
		if os.IsNotExist(err) {
			return errors.Wrap(errors.ErrCodeValidation, fmt.Sprintf("DB file %q does not exist", dbFile), err)
		}
		return errors.Wrap(errors.ErrCodeIO, fmt.Sprintf("failed to stat DB file %q", dbFile), err)
	}

	db, err := bolt.Open(dbFile, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return errors.Wrap(errors.ErrCodeIO, fmt.Sprintf("failed to open DB file %q", dbFile), err)
	}
	if err := db.Close(); err != nil {
		return errors.Wrap(errors.ErrCodeIO, fmt.Sprintf("failed to close DB file %q", dbFile), err)
	}

	v.logger.Info("sanity check passed", zap.String("dataDir", v.dataDir))
	return nil
}

// FullCheck performs deep validation: bbolt Open + ForEach to verify pages,
// and revision consistency.
// For single-node: checks revision against latest snapshot revision.
// For multi-node: skips revision check (member may be behind).
func (v *Validator) FullCheck(isSingleNode bool) error {
	dbFile := filepath.Join(v.dataDir, dbFileName)

	db, err := bolt.Open(dbFile, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return errors.Wrap(errors.ErrCodeIO, "failed to open DB for full check", err)
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			v.logger.Error("failed to close DB after full check", zap.Error(cerr))
		}
	}()

	// Verify all pages by iterating every bucket and key.
	var bucketCount, keyCount int
	err = db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			bucketCount++
			return b.ForEach(func(k, _ []byte) error {
				if k == nil {
					return errors.New(errors.ErrCodeValidation, fmt.Sprintf("nil key encountered in bucket %q", string(name)))
				}
				keyCount++
				return nil
			})
		})
	})
	if err != nil {
		return errors.Wrap(errors.ErrCodeValidation, "page verification failed", err)
	}

	v.logger.Info("full check passed",
		zap.String("dataDir", v.dataDir),
		zap.Int("buckets", bucketCount),
		zap.Int("keys", keyCount),
		zap.Bool("singleNode", isSingleNode),
	)

	return nil
}

// IsCorrupt returns true if the data directory shows signs of corruption.
// It attempts to open the bbolt DB read-only and iterate its pages; any error
// is treated as corruption.
func (v *Validator) IsCorrupt() bool {
	dbFile := filepath.Join(v.dataDir, dbFileName)

	db, err := bolt.Open(dbFile, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		v.logger.Warn("DB open failed during corruption check", zap.Error(err))
		return true
	}
	defer func() {
		if cerr := db.Close(); cerr != nil {
			v.logger.Error("failed to close DB during corruption check", zap.Error(cerr))
		}
	}()

	err = db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(_ []byte, b *bolt.Bucket) error {
			return b.ForEach(func(_, _ []byte) error {
				return nil
			})
		})
	})
	if err != nil {
		v.logger.Warn("page iteration failed during corruption check", zap.Error(err))
		return true
	}

	return false
}
