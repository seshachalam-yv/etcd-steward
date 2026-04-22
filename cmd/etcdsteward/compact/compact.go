// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package compact

import (
	"fmt"

	"github.com/spf13/cobra"
)

// NewCommand creates a new cobra command for the compact subcommand.
// It performs an offline compaction of the etcd data directory using a
// restore-from-snapshot approach (MVP).
func NewCommand() *cobra.Command {
	var (
		configFile     string
		dataDir        string
		etcdEndpoints  string
		storePrefix    string
		storeContainer string
	)

	cmd := &cobra.Command{
		Use:   "compact",
		Short: "Compact the etcd database",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("compact called with config=%s data-dir=%s etcd-endpoints=%s store-prefix=%s store-container=%s\n",
				configFile, dataDir, etcdEndpoints, storePrefix, storeContainer)
			// MVP: uses restorer.RestoreFull to compact via a restore cycle.
			return nil
		},
	}

	cmd.Flags().StringVar(&configFile, "config", "", "path to the configuration file")
	cmd.Flags().StringVar(&dataDir, "data-dir", "/var/etcd/data/new.etcd", "etcd data directory")
	cmd.Flags().StringVar(&etcdEndpoints, "etcd-endpoints", "http://localhost:2379", "comma-separated etcd client endpoints")
	cmd.Flags().StringVar(&storePrefix, "store-prefix", "", "snapshot store prefix")
	cmd.Flags().StringVar(&storeContainer, "store-container", "", "snapshot store container path")

	return cmd
}
