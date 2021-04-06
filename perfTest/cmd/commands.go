package cmd

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/gsantomaggio/go-stream-client/pkg/streaming"
	"github.com/spf13/cobra"
	"os"
)

var rootCmd = &cobra.Command{
	Use:           "PerfTest",
	Short:         fmt.Sprintf(`RabbitMQ Golang PerfTest for streaming queues `),
	SilenceUsage:  true,
	SilenceErrors: true,
}

var (
	rabbitmqBrokerUrl string
	producers         int
	consumers         int
	preDeclared       bool
	streams           []string
	maxLengthBytes    string
)

func init() {
	setupCli(rootCmd)
}

func setupCli(baseCmd *cobra.Command) {
	baseCmd.PersistentFlags().StringVarP(&rabbitmqBrokerUrl, "uris", "u", streaming.LocalhostUriConnection, "Broker URL")
	baseCmd.PersistentFlags().IntVarP(&producers, "producers", "p", 1, "Number of Producers")
	baseCmd.PersistentFlags().IntVarP(&consumers, "consumers", "c", 1, "Number of Consumers")
	baseCmd.PersistentFlags().BoolVarP(&preDeclared, "pre-declared", "d", false, "Pre created stream")
	baseCmd.PersistentFlags().StringSliceVarP(&streams, "streams", "s", []string{uuid.New().String()}, "Stream names, create an UUID if not specified")
	baseCmd.PersistentFlags().StringVarP(&maxLengthBytes, "max_length_bytes", "m", "", "Stream max length bytes, unlimited")
	baseCmd.AddCommand(versionCmd)
	baseCmd.AddCommand(newSilent())
}

//Execute is the entrypoint of the commands
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

}
