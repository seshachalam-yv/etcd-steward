// SPDX-FileCopyrightText: 2025 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package copybackups

import (
	"fmt"

	"github.com/spf13/cobra"
)

// NewCommand creates a new cobra command for the copy-backups subcommand.
// It copies snapshots between storage backends (currently local provider only).
func NewCommand() *cobra.Command {
	var (
		sourcePrefix    string
		sourceContainer string
		destPrefix      string
		destContainer   string
		sourceProvider  string
		destProvider    string
	)

	cmd := &cobra.Command{
		Use:   "copy-backups",
		Short: "Copy etcd backups between storage backends",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Printf("copy-backups called with source-prefix=%s source-container=%s dest-prefix=%s dest-container=%s source-provider=%s dest-provider=%s\n",
				sourcePrefix, sourceContainer, destPrefix, destContainer, sourceProvider, destProvider)
			// MVP: local provider copy only.
			return nil
		},
	}

	cmd.Flags().StringVar(&sourcePrefix, "source-prefix", "", "source snapshot store prefix")
	cmd.Flags().StringVar(&sourceContainer, "source-container", "", "source snapshot store container path")
	cmd.Flags().StringVar(&destPrefix, "dest-prefix", "", "destination snapshot store prefix")
	cmd.Flags().StringVar(&destContainer, "dest-container", "", "destination snapshot store container path")
	cmd.Flags().StringVar(&sourceProvider, "source-provider", "Local", "source storage provider")
	cmd.Flags().StringVar(&destProvider, "dest-provider", "Local", "destination storage provider")

	return cmd
}
