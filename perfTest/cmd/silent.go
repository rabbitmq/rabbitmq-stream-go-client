package cmd

import (
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	"github.com/gsantomaggio/go-stream-client/pkg/streaming"
	"github.com/spf13/cobra"
	"math/rand"
	"sync/atomic"
	"time"
)

func newSilent() *cobra.Command {
	var silentCmd = &cobra.Command{
		Use:   "silent",
		Short: "Start a silent simulation",
		RunE: func(cmd *cobra.Command, args []string) error {
			return startSimulation()
		},
	}
	return silentCmd
}

var (
	producerMessageCount int32
	consumerMessageCount int32
	connections          []*streaming.Client
)

func printStats() {
	if printStatsV {
		start := time.Now()
		ticker := time.NewTicker(2 * time.Second)
		go func() {
			for {
				select {
				case _ = <-ticker.C:
					v := time.Now().Sub(start).Seconds()
					PMessagesPerSecond := float64(atomic.LoadInt32(&producerMessageCount)) / v
					CMessagesPerSecond := float64(atomic.LoadInt32(&consumerMessageCount)) / v
					streaming.INFO("Published %.2f msg/s, Consumed %.2f msg/s, Active Connections: %d", PMessagesPerSecond, CMessagesPerSecond, len(connections))
					atomic.SwapInt32(&producerMessageCount, 0)
					atomic.SwapInt32(&consumerMessageCount, 0)
					start = time.Now()
				}
			}

		}()
	}
}

func startSimulation() error {
	streaming.INFO("Silent (%s) Simulation, url: %s producers: %d consumers: %d streams: %s ", streaming.Version, rabbitmqBrokerUrl, producers, consumers, streams)

	err := initStreams()
	err = startConsumers()
	err = startProducers()
	printStats()

	return err
}

func randomSleep() {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(10) // n will be between 0 and 10
	time.Sleep(time.Duration(n) * time.Second)
}

func initStreams() error {
	if !preDeclared {
		streaming.INFO("Create streams :%s\n", streams)
		client, err := streaming.NewClientCreator().
			Uri(rabbitmqBrokerUrl).Connect()
		if err != nil {
			streaming.ERROR("Error init stream connection: %s", err)
			return err
		}

		for _, stream := range streams {

			err = client.StreamCreator().
				Stream(stream).
				MaxLengthBytes(streaming.ByteCapacity{}.From(maxLengthBytes)).
				Create()
			if err != nil {
				streaming.ERROR("Error creating stream: %s", err)
				_ = client.Close()
				return err
			}
		}
		streaming.INFO("End Init streams :%s\n", streams)
		return client.Close()
	}
	streaming.INFO("Predeclared streams :%s\n", streams)
	return nil
}
func startProducers() error {
	streaming.INFO("Create producers :%d\n", producers)
	for _, stream := range streams {
		for i := 0; i < producers; i++ {
			client, err := streaming.NewClientCreator().Uri(rabbitmqBrokerUrl).Connect()
			if err != nil {
				streaming.ERROR("Error connection client producer: %s", err)
				return err
			}
			connections = append(connections, client)
			producer, err := client.ProducerCreator().Stream(stream).Build()
			if err != nil {
				streaming.ERROR("Error create producer: %s", err)
				return err
			}
			var arr []*amqp.Message
			for z := 0; z < batchSize; z++ {

				arr = append(arr, amqp.NewMessage([]byte(fmt.Sprintf("simul_%s", stream)  )))
			}

			go func(prod *streaming.Producer, messages []*amqp.Message) {
				for {
					if rate > 0 {
						sleep := float64(batchSize) / float64(rate)
						sleep = sleep * 1000
						time.Sleep(time.Duration(sleep) * time.Millisecond)
					}
					atomic.AddInt32(&producerMessageCount, 100)
					_, err = prod.BatchPublish(nil, arr)
					if err != nil {
						streaming.ERROR("Error publishing %s", err)
						time.Sleep(1 * time.Second)
					}
				}
			}(producer, arr)
		}
	}
	return nil
}

func startConsumers() error {
	streaming.INFO("Start Consumers  :%d\n", consumers)
	for _, stream := range streams {
		for i := 0; i < consumers; i++ {
			client, err := streaming.NewClientCreator().Uri(rabbitmqBrokerUrl).Connect()

			if err != nil {
				streaming.ERROR("Error creating consumer connection: %s", err)
				return err
			}
			connections = append(connections, client)
			randomSleep()
			_, err = client.ConsumerCreator().Stream(stream).
				Offset(streaming.OffsetSpecification{}.Last()).
				Name(uuid.New().String()).
				MessagesHandler(func(Context streaming.ConsumerContext, message *amqp.Message) {
					if atomic.AddInt32(&consumerMessageCount, 1)%500 == 0 {
						err := Context.Consumer.Commit()
						if err != nil {
							streaming.ERROR("Error Commit: %s", err)
						}
					}
				}).Build()
			if err != nil {
				streaming.ERROR("Error creating consumer: %s", err)
				//return err
			}

		}
	}
	return nil
}
