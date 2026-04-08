package cmd

import (
	"fmt"
	"os"

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
	if ansiEnabled() {
		_, _ = fmt.Fprintf(os.Stdout,
			"%s\n%s %s\n%s %s\n%s %s\n",
			style(ansiBold+ansiHiCyan, "RabbitMQ Stream PerfTest"),
			style(ansiDim+ansiGray, "version:   "), style(ansiBold+ansiWhite, version),
			style(ansiDim+ansiGray, "go:        "), style(ansiGreen, goVersion),
			style(ansiDim+ansiGray, "commit:    "), style(ansiMagenta, commit),
		)
		return
	}
	fmt.Printf("version: %s\ngo version: %s\ncommit: %s\n",
		version,
		goVersion,
		commit,
	)
}
