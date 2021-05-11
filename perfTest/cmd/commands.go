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
	Short:         "RabbitMQ Golang PerfTest for streaming queues",
	SilenceUsage:  true,
	SilenceErrors: true,
}

var (
	rabbitmqBrokerUrl  string
	producers          int
	consumers          int
	producersPerClient int
	consumersPerClient int
	preDeclared        bool
	streams            []string
	maxLengthBytes     string
	printStatsV        bool
	rate               int
	variableRate       int
	variableBody       int
	batchSize          int
	exitOnError        bool
)

func init() {
	setupCli(rootCmd)
}

func setupCli(baseCmd *cobra.Command) {
	batchSize = 100
	baseCmd.PersistentFlags().StringVarP(&rabbitmqBrokerUrl, "uris", "u", streaming.LocalhostUriConnection, "Broker URL")
	baseCmd.PersistentFlags().IntVarP(&producers, "producers", "p", 1, "Number of Producers")
	baseCmd.PersistentFlags().IntVarP(&consumers, "consumers", "c", 1, "Number of Consumers")
	baseCmd.PersistentFlags().IntVarP(&producersPerClient, "producers-per-client", "k", 13, "producers Per Client")
	baseCmd.PersistentFlags().IntVarP(&consumersPerClient, "consumers-per-client", "j", 15, "consumers Per Client")
	baseCmd.PersistentFlags().IntVarP(&rate, "rate", "r", 0, "Limit publish rate")
	baseCmd.PersistentFlags().IntVarP(&variableRate, "variable-rate", "v", 0, "Variable rate to value")
	baseCmd.PersistentFlags().IntVarP(&variableBody, "variable-body", "b", 0, "Variable body size")
	baseCmd.PersistentFlags().BoolVarP(&preDeclared, "pre-declared", "d", false, "Pre created stream")
	baseCmd.PersistentFlags().BoolVarP(&exitOnError, "exit-on-error", "x", true, "Close the app in case of error")
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
