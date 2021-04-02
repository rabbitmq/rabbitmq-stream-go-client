package cmd

import (
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
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
	if !pre_created {
		fmt.Printf("-Init streams :%s\n", streams)
		client, err := streaming.NewClientCreator().Uri(rabbitmqBrokerUrl).Connect()
		if err != nil {
			fmt.Printf("ERROR :%s\n", err)
			return err
		}

		for _, stream := range streams {

			err = client.StreamCreator().Stream(stream).Create()
			if err != nil {
				fmt.Printf("ERROR :%s\n", err)
				return err
			}
		}
		fmt.Printf("End Init streams :%s\n", streams)
		return client.Close()
	}
	fmt.Printf("Pre Created streams :%s\n", streams)
	return nil
}
func startProducers() error {
	fmt.Printf("-Init Producers :%d\n", producers)
	for _, stream := range streams {
		client, err := streaming.NewClientCreator().Uri(rabbitmqBrokerUrl).Connect()
		if err != nil {
			fmt.Printf("ERROR :%s\n", err)
			return err
		}
		fmt.Printf("-Init Producers1  :%d\n", producers)
		for i := 0; i < producers; i++ {

			producer, err := client.ProducerCreator().Stream(stream).Build()
			if err != nil {
				fmt.Printf("ERROR :%s\n", err)
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
					if count%500_000 == 0 {
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
	fmt.Printf("-Init Consumers  :%d\n", consumers)
	for _, stream := range streams {
		for i := 0; i < consumers; i++ {
			client, err := streaming.NewClientCreator().Uri(rabbitmqBrokerUrl).Connect()
			if err != nil {
				return err
			}
			for subConsumer := 0; subConsumer < 2; subConsumer++ {
				counters := make(map[uint8]int64)
				var mutex sync.Mutex
				start := time.Now()
				_, err = client.ConsumerCreator().Stream(stream).
					Offset(streaming.OffsetSpecification{}.Last()).
					Name(uuid.New().String()).
					MessagesHandler(func(Context streaming.ConsumerContext, message *amqp.Message) {
						mutex.Lock()
						defer mutex.Unlock()
						counters[Context.Consumer.ID] = counters[Context.Consumer.ID] + 1
						if counters[Context.Consumer.ID]%500_000 == 0 {
							elapsed := time.Since(start)
							fmt.Printf("%d messages, consumed in: %s on the stream %s\n", counters[Context.Consumer.ID], elapsed,
								Context.Consumer.GetStream())
							Context.Consumer.Commit()
							time.Sleep(500 * time.Millisecond)
						}

					}).Build()
				if err != nil {
					fmt.Printf("ERROR :%s\n", err)
					return err
				}
			}

		}
	}
	return nil
}
