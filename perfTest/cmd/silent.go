package cmd

import (
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/gsantomaggio/go-stream-client/pkg/streaming"
	"github.com/spf13/cobra"
	"sync"
	"time"
)

func newSilent() *cobra.Command {
	var silentCmd = &cobra.Command{
		Use:   "silent",
		Short: "Start a silent simulation",
		RunE: func(cmd *cobra.Command, args []string) error {
			return startSimul()
		},
	}
	return silentCmd
}

func startSimul() error {
	fmt.Printf("Silent Simulation, url: %s producers: %d consumers: %d streams :%s\n", rabbitmqBrokerUrl, producers, consumers, streams)
	err := initStreams()
	err = startProducers()
	err = startConsumers()
	return err
}

func initStreams() error {
	for _, stream := range streams {
		client, err := streaming.NewClientCreator().Uri(rabbitmqBrokerUrl).Connect()
		if err != nil {
			return err
		}
		err = client.StreamCreator().Stream(stream).Create()
		if err != nil {
			return err
		}
	}
	return nil
}
func startProducers() error {
	for _, stream := range streams {

		for i := 0; i < producers; i++ {
			client, err := streaming.NewClientCreator().Uri(rabbitmqBrokerUrl).Connect()
			if err != nil {
				return err
			}
			producer, err := client.ProducerCreator().Stream(stream).Build()
			if err != nil {
				return err
			}
			go func(prod *streaming.Producer, streamC string) {
				var count int64
				start := time.Now()
				for {
					var arr []*amqp.Message
					for z := 0; z < 100; z++ {
						count++
						arr = append(arr, amqp.NewMessage([]byte(fmt.Sprintf("simul_message_stream%s, %d", streamC, count)  )))
					}
					_, err = prod.BatchPublish(nil, arr)
					if err != nil {
						fmt.Printf("Error publishing ")
						time.Sleep(1 * time.Second)
					}
					if count%1_000_000 == 0 {
						elapsed := time.Since(start)
						fmt.Printf("%d messages, published in: %s on the stream %s\n", count, elapsed, streamC)
					}
				}
			}(producer, stream)
		}
	}
	return nil
}

func startConsumers() error {
	for _, stream := range streams {

		for i := 0; i < consumers; i++ {
			client, err := streaming.NewClientCreator().Uri(rabbitmqBrokerUrl).Connect()
			if err != nil {
				return err
			}

			counters := make(map[uint8]int64)
			var mutex sync.Mutex
			start:= time.Now()
			_, err = client.ConsumerCreator().Stream(stream).
				MessagesHandler(func(Context streaming.ConsumerContext, message *amqp.Message) {
					mutex.Lock()
					defer mutex.Unlock()
					counters[Context.Consumer.ID] = counters[Context.Consumer.ID] + 1
					if counters[Context.Consumer.ID]%1_000_000 == 0 {
						elapsed := time.Since(start)
						fmt.Printf("%d messages, consumed in: %s on the stream %s\n", counters[Context.Consumer.ID], elapsed,
							Context.Consumer.GetStream())
					}

				}).Build()
			if err != nil {
				return err
			}

		}
	}
	return nil
}
