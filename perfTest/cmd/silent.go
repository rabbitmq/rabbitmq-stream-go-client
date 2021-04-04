package cmd

import (
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	"github.com/gsantomaggio/go-stream-client/pkg/streaming"
	"github.com/spf13/cobra"
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
)

func printStats() {

	ticker := time.NewTicker(3 * time.Second)
	go func() {
		for {
			select {
			case _ = <-ticker.C:
				PMessagesPerSecond := atomic.LoadInt32(&producerMessageCount) / 3
				CMessagesPerSecond := atomic.LoadInt32(&consumerMessageCount) / 3
				streaming.INFO("Published %d msg/s, Consumed %d msg/s", PMessagesPerSecond, CMessagesPerSecond)
				atomic.AddInt32(&producerMessageCount, -atomic.LoadInt32(&producerMessageCount))
				atomic.AddInt32(&consumerMessageCount, -atomic.LoadInt32(&consumerMessageCount))
			}
		}

	}()
}

func startSimulation() error {
	streaming.INFO("Silent Simulation, url: %s producers: %d consumers: %d streams :%s\n", rabbitmqBrokerUrl, producers, consumers, streams)
	err := initStreams()
	err = startProducers()
	err = startConsumers()
	printStats()
	return err
}

func initStreams() error {
	if !preDeclared {
		streaming.INFO("Create streams :%s\n", streams)
		client, err := streaming.NewClientCreator().Uri(rabbitmqBrokerUrl).Connect()
		if err != nil {
			return err
		}

		for _, stream := range streams {

			err = client.StreamCreator().Stream(stream).Create()
			if err != nil {
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
		client, err := streaming.NewClientCreator().Uri(rabbitmqBrokerUrl).Connect()
		if err != nil {
			return err
		}
		for i := 0; i < producers; i++ {

			producer, err := client.ProducerCreator().Stream(stream).Build()
			if err != nil {
				return err
			}

			go func(prod *streaming.Producer, streamC string) {
				for {
					var arr []*amqp.Message
					for z := 0; z < 100; z++ {
						atomic.AddInt32(&producerMessageCount, 1)
						arr = append(arr, amqp.NewMessage([]byte(fmt.Sprintf("simul_%s", streamC)  )))
					}
					_, err = prod.BatchPublish(nil, arr)
					if err != nil {
						streaming.ERROR("Error publishing %s", err)
						time.Sleep(1 * time.Second)
					}
				}
			}(producer, stream)
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
				return err
			}
			_, err = client.ConsumerCreator().Stream(stream).
				Offset(streaming.OffsetSpecification{}.Last()).
				Name(uuid.New().String()).
				MessagesHandler(func(Context streaming.ConsumerContext, message *amqp.Message) {
					if atomic.AddInt32(&consumerMessageCount, 1)%500 == 0 {
						Context.Consumer.Commit()
					}
				}).Build()
			if err != nil {
				streaming.ERROR("%s", err)
				return err
			}

		}
	}
	return nil
}
