package cmd

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/streaming"
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
	printStatsV       bool
	rate              int
	batchSize         int
)

func init() {
	setupCli(rootCmd)
}

func setupCli(baseCmd *cobra.Command) {
	batchSize = 100
	baseCmd.PersistentFlags().StringVarP(&rabbitmqBrokerUrl, "uris", "u", streaming.LocalhostUriConnection, "Broker URL")
	baseCmd.PersistentFlags().IntVarP(&producers, "producers", "p", 1, "Number of Producers")
	baseCmd.PersistentFlags().IntVarP(&consumers, "consumers", "c", 1, "Number of Consumers")
	baseCmd.PersistentFlags().IntVarP(&rate, "rate", "r", 0, "Limit publish rate")
	baseCmd.PersistentFlags().BoolVarP(&preDeclared, "pre-declared", "d", false, "Pre created stream")
	baseCmd.PersistentFlags().BoolVarP(&printStatsV, "print-stats", "n", true, "Print stats")
	baseCmd.PersistentFlags().StringSliceVarP(&streams, "streams", "s", []string{uuid.New().String()}, "Stream names, create an UUID if not specified")
	baseCmd.PersistentFlags().StringVarP(&maxLengthBytes, "max_length_bytes", "m", "", "Stream max length bytes, default is unlimited, ex: 10MB,50GB, etc..")
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
