package cmd

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/spf13/cobra"
	"log"
	"os"
)

func logInfo(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[info] - %s", message), v...)
}
func logError(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[error] - %s", message), v...)
}

var rootCmd = &cobra.Command{
	Use:           "PerfTest",
	Short:         "RabbitMQ Golang PerfTest for streaming queues",
	SilenceUsage:  true,
	SilenceErrors: true,
}

var (
	rabbitmqBrokerUrl   string
	publishers          int
	consumers           int
	publishersPerClient int
	consumersPerClient  int
	preDeclared         bool
	streams             []string
	maxLengthBytes      string
	maxSegmentSizeBytes string
	printStatsV         bool
	rate                int
	variableRate        int
	variableBody        int
	batchSize           int
	exitOnError         bool
)

func init() {
	setupCli(rootCmd)
}

func setupCli(baseCmd *cobra.Command) {
	baseCmd.PersistentFlags().StringVarP(&rabbitmqBrokerUrl, "uris", "", stream.LocalhostUriConnection, "Broker URL")
	baseCmd.PersistentFlags().IntVarP(&publishers, "publishers", "", 1, "Number of Publishers")
	baseCmd.PersistentFlags().IntVarP(&batchSize, "batch-size", "", 100, "Batch Size, from 1 to 200")
	baseCmd.PersistentFlags().IntVarP(&consumers, "consumers", "", 1, "Number of Consumers")
	baseCmd.PersistentFlags().IntVarP(&publishersPerClient, "publishers-per-client", "", 3, "Publishers Per Client")
	baseCmd.PersistentFlags().IntVarP(&consumersPerClient, "consumers-per-client", "", 3, "Consumers Per Client")
	baseCmd.PersistentFlags().IntVarP(&rate, "rate", "", 0, "Limit publish rate")
	baseCmd.PersistentFlags().IntVarP(&variableRate, "variable-rate", "", 0, "Variable rate to value")
	baseCmd.PersistentFlags().IntVarP(&variableBody, "variable-body", "", 0, "Variable body size")
	baseCmd.PersistentFlags().BoolVarP(&preDeclared, "pre-declared", "", false, "Pre created stream")
	baseCmd.PersistentFlags().BoolVarP(&exitOnError, "exit-on-error", "", true, "Close the app in case of error")
	baseCmd.PersistentFlags().BoolVarP(&printStatsV, "print-stats", "", true, "Print stats")
	baseCmd.PersistentFlags().StringSliceVarP(&streams, "streams", "", []string{"perf-test-go"}, "Stream names")
	baseCmd.PersistentFlags().StringVarP(&maxLengthBytes, "max-length-bytes", "", "1GB", "Stream max length bytes, e.g. 10MB, 50GB, etc.")
	baseCmd.PersistentFlags().StringVarP(&maxSegmentSizeBytes, "stream-max-segment-size-bytes", "", "500MB", "Stream segment size bytes, e.g. 10MB, 1GB, etc.")
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
