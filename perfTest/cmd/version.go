package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// These values are overridden at build time.
// Please update ldflags aptly when renaming these vars or moving packages
var (
	version   = "dev"
	commit    = "dev"
	goVersion = "unknown"
)
var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print CLI version",
	Run: func(_ *cobra.Command, _ []string) {
		printVersion()
	},
}

func printVersion() {
	fmt.Printf("version: %s\ngo version: %s\ncommit: %s\n",
		version,
		goVersion,
		commit,
	)
}
