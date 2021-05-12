package cmd

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/streaming"
	"github.com/spf13/cobra"
	"math/rand"
	"os"
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
	producerMessageCount  int32
	consumerMessageCount  int32
	confirmedMessageCount int32
	connections           []*stream.Client
)

func printStats() {
	if printStatsV {
		start := time.Now()
		ticker := time.NewTicker(3 * time.Second)
		go func() {
			for {
				select {
				case _ = <-ticker.C:
					v := time.Now().Sub(start).Seconds()
					PMessagesPerSecond := int64(float64(atomic.LoadInt32(&producerMessageCount)) / v)
					CMessagesPerSecond := int64(float64(atomic.LoadInt32(&consumerMessageCount)) / v)
					ConfirmedMessagesPerSecond := int64(float64(atomic.LoadInt32(&confirmedMessageCount)) / v)
					stream.INFO("Published %8v msg/s   |   Confirmed %8v msg/s   |   Consumed %3v msg/s   |  %3v  |  %3v  |",
						PMessagesPerSecond, ConfirmedMessagesPerSecond, CMessagesPerSecond, decodeRate(), decodeBody())
					atomic.SwapInt32(&producerMessageCount, 0)
					atomic.SwapInt32(&consumerMessageCount, 0)
					atomic.SwapInt32(&confirmedMessageCount, 0)
					start = time.Now()
				}
			}

		}()
	}
}

func decodeBody() string {
	if producers > 0 {
		if variableBody > 0 {
			return fmt.Sprintf("Variable Body: %d", variableBody)
		}
		return fmt.Sprintf("Fixed Body: %d", len("simul_message"))
	} else {
		return "ND"
	}
}

func decodeRate() string {
	if producers > 0 {
		if rate > 0 {
			return fmt.Sprintf("Fixed Rate: %d", rate)
		}
		if variableRate > 0 {
			return fmt.Sprintf("Variable Rate: %d", variableRate)
		}
		return "Full rate"
	} else {
		return "ND"
	}
}

func startSimulation() error {
	stream.INFO("Silent (%s) Simulation, url: %s producers: %d consumers: %d streams: %s ", stream.ClientVersion, rabbitmqBrokerUrl, producers, consumers, streams)

	err := initStreams()
	if err != nil {
		if exitOnError {
			os.Exit(1)
		}
	}
	if consumers > 0 {
		err = startConsumers()
		if err != nil {
			if exitOnError {
				os.Exit(1)
			}
		}
	}
	if producers > 0 {
		err = startProducers()
		if err != nil {
			if exitOnError {
				os.Exit(1)
			}
		}
	}
	printStats()

	return err
}

func randomSleep() {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(500) // n will be between 0 and 2
	time.Sleep(time.Duration(n) * time.Millisecond)
}

func initStreams() error {
	if !preDeclared {
		stream.INFO("Declaring streams: %s", streams)
		env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().Uri(
			rabbitmqBrokerUrl))
		if err != nil {
			stream.ERROR("Error init stream connection: %s", err)
			return err
		}

		for _, stream := range streams {

			err = env.DeclareStream(stream, stream.NewStreamOptions().
				MaxLengthBytes(stream.ByteCapacity{}.From(maxLengthBytes)))
			if err != nil {
				stream.ERROR("Error declaring stream: %s", err)
				_ = env.Close()
				return err
			}
		}
		stream.INFO("End Init streams :%s\n", streams)
		return env.Close()
	}
	stream.INFO("Predeclared streams: %s\n", streams)
	return nil
}
func startProducers() error {
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().Uri(
		rabbitmqBrokerUrl).MaxProducersPerClient(producersPerClient))
	if err != nil {
		stream.ERROR("Error connection client producer: %s", err)
		return err
	}
	stream.INFO("Starting %d producers...", producers)
	for _, stream := range streams {
		for i := 1; i <= producers; i++ {

			stream.INFO("Starting producer number: %d", i)
			producer, err := env.NewProducer(stream, stream.NewProducerOptions().OnPublishConfirm(func(ch <-chan []int64) {
				ids := <-ch
				atomic.AddInt32(&confirmedMessageCount, int32(len(ids)))
			}))
			if err != nil {
				stream.ERROR("Error create producer: %s", err)
				return err
			}

			var arr []*amqp.Message
			var body string
			for z := 0; z < batchSize; z++ {
				if variableBody > 0 {
					rand.Seed(time.Now().UnixNano())
					n := rand.Intn(variableBody)
					for i := 0; i < n; i++ {
						body += "s"
					}
				} else {
					body = fmt.Sprintf("simul_message")
				}

				arr = append(arr, amqp.NewMessage([]byte(body)))
			}

			go func(prod *stream.Producer, messages []*amqp.Message) {
				for {
					if rate > 0 {
						sleep := float64(batchSize) / float64(rate)
						sleep = sleep * 1000
						time.Sleep(time.Duration(sleep) * time.Millisecond)
					}

					if variableRate > 0 {
						rand.Seed(time.Now().UnixNano())
						n := rand.Intn(variableRate)
						sleep := float64(batchSize) / float64(n)

						sleep = sleep * 1000
						if sleep > 3000 {
							sleep = 0
						}
						time.Sleep(time.Duration(sleep) * time.Millisecond)
					}

					atomic.AddInt32(&producerMessageCount, 100)
					_, err = prod.BatchPublish(context.Background(), arr)
					if err != nil {
						stream.ERROR("Error publishing %s", err)
						time.Sleep(1 * time.Second)
					}
				}
			}(producer, arr)
		}
	}
	return nil
}

func startConsumers() error {
	stream.INFO("Starting %d consumers...", consumers)
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().Uri(
		rabbitmqBrokerUrl).MaxConsumersPerClient(consumersPerClient))
	if err != nil {
		stream.ERROR("Error creating consumer connection: %s", err)
		stream.ERROR("ENV %+v", env)
		return err
	}

	for _, stream := range streams {
		for i := 0; i < consumers; i++ {
			randomSleep()
			stream.INFO("Starting consumer number: %d", i)
			_, err = env.NewConsumer(stream, func(Context stream.ConsumerContext, message *amqp.Message) {
				if atomic.AddInt32(&consumerMessageCount, 1)%500 == 0 {
					err := Context.Consumer.Commit()
					if err != nil {
						stream.ERROR("Error Commit: %s", err)
					}
				}
			}, stream.NewConsumerOptions().
				Offset(stream.OffsetSpecification{}.First()).
				Name(uuid.New().String()))
			if err != nil {
				stream.ERROR("Error creating consumer: %s", err)
				//return err
			}

		}
	}
	return nil
}
