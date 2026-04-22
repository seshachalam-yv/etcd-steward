// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/gardener/etcd-steward/cmd/etcdsteward/compact"
	"github.com/gardener/etcd-steward/cmd/etcdsteward/copybackups"
	"github.com/gardener/etcd-steward/internal/config"
	"github.com/gardener/etcd-steward/internal/snapstore"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// Version is set via ldflags at build time.
var Version = "dev"

func newRootCommand() *cobra.Command {
	cfg := config.DefaultConfig()

	root := &cobra.Command{
		Use:   "etcd-steward",
		Short: "etcd-steward manages etcd operational tasks",
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfg.ConfigFile != "" {
				if err := config.LoadFromFile(cfg, cfg.ConfigFile, cmd.Flags()); err != nil {
					return fmt.Errorf("loading config file: %w", err)
				}
			}

			store, err := snapstore.NewSnapStore(cfg.StoreProvider, cfg.StorePrefix, cfg.StoreContainer, nil)
			if err != nil {
				return fmt.Errorf("creating snapstore: %w", err)
			}

			fmt.Printf("etcd-steward daemon starting with %s snapstore (container=%s, prefix=%s)\n",
				cfg.StoreProvider, cfg.StoreContainer, cfg.StorePrefix)
			_ = store // TODO: pass store to daemon components
			return nil
		},
	}

	config.BindFlags(cfg, root.PersistentFlags())
	// Ensure pflags are also visible via root.Flags() for direct sub-command use.
	root.Flags().AddFlagSet(root.PersistentFlags())

	root.AddCommand(newVersionCommand())
	root.AddCommand(compact.NewCommand())
	root.AddCommand(copybackups.NewCommand())

	return root
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version of etcd-steward",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(Version)
		},
	}
}

func main() {
	pflag.CommandLine.AddGoFlagSet(nil) // ensure pflag is initialized
	if err := newRootCommand().Execute(); err != nil {
		os.Exit(1)
	}
}
