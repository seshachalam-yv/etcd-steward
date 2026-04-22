// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/gardener/etcd-steward/cmd/etcdsteward/compact"
	"github.com/gardener/etcd-steward/cmd/etcdsteward/copybackups"
	"github.com/spf13/cobra"
)

// Version is set via ldflags at build time.
var Version = "dev"

func newRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "etcd-steward",
		Short: "etcd-steward manages etcd operational tasks",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("etcd-steward daemon starting...")
		},
	}

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
	if err := newRootCommand().Execute(); err != nil {
		os.Exit(1)
	}
}
