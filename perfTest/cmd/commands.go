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
	streams           []string
)

// Settings default user setting
type Settings struct {
	rabbitmqBrokerUrl string `yaml:"rabbitmq_broker_url"`
}

func init() {
	setupCli(rootCmd)
}

func setupCli(baseCmd *cobra.Command) {
	//setting := &Settings{}
	baseCmd.PersistentFlags().StringVarP(&rabbitmqBrokerUrl, "uris", "u", streaming.LocalhostUriConnection, "Broker URL")
	baseCmd.PersistentFlags().IntVarP(&producers, "producers", "p", 1, "Number of Producers")
	baseCmd.PersistentFlags().IntVarP(&consumers, "consumers", "c", 1, "Number of Consumers")
	baseCmd.PersistentFlags().StringSliceVarP(&streams, "streams", "s", []string{uuid.New().String()}, "Stream names, create an UUID if not specified")
	baseCmd.AddCommand(versionCmd)
	baseCmd.AddCommand(newSilent())
	//baseCmd.AddCommand(newListCommand())
	//baseCmd.AddCommand(newInitCommand())
	//setting.rabbitmqBrokerUrl = rabbitmqBrokerUrl
}

//Execute is the entrypoint of the commands
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}


}
