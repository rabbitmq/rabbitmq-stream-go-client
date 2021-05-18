package cmd

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
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
	publisherMessageCount int32
	consumerMessageCount  int32
	confirmedMessageCount int32
	//connections           []*stream.Client
	simulEnvironment stream.Environment
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
					PMessagesPerSecond := int64(float64(atomic.LoadInt32(&publisherMessageCount)) / v)
					CMessagesPerSecond := int64(float64(atomic.LoadInt32(&consumerMessageCount)) / v)
					ConfirmedMessagesPerSecond := int64(float64(atomic.LoadInt32(&confirmedMessageCount)) / v)
					logInfo("Published %8v msg/s   |   Confirmed %8v msg/s   |   Consumed %3v msg/s   |  %3v  |  %3v  |",
						PMessagesPerSecond, ConfirmedMessagesPerSecond, CMessagesPerSecond, decodeRate(), decodeBody())
					atomic.SwapInt32(&publisherMessageCount, 0)
					atomic.SwapInt32(&consumerMessageCount, 0)
					atomic.SwapInt32(&confirmedMessageCount, 0)
					start = time.Now()
				}
			}

		}()
	}
}

func decodeBody() string {
	if publishers > 0 {
		if variableBody > 0 {
			return fmt.Sprintf("Variable Body: %d", variableBody)
		}
		return fmt.Sprintf("Fixed Body: %d", len("simul_message"))
	} else {
		return "ND"
	}
}

func decodeRate() string {
	if publishers > 0 {
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
<<<<<<< HEAD
	logInfo("Silent (%s) Simulation, url: %s publishers: %d consumers: %d streams: %s ", stream.ClientVersion, rabbitmqBrokerUrl, publishers, consumers, streams)

	err := initStreams()
=======
	logInfo("Silent (%s) Simulation, url: %s producers: %d consumers: %d streams: %s ", stream.ClientVersion, rabbitmqBrokerUrl, producers, consumers, streams)
	var err error
	simulEnvironment, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetUri(
		rabbitmqBrokerUrl).SetMaxProducersPerClient(producersPerClient).SetMaxConsumersPerClient(consumersPerClient))
	if err != nil {
		if exitOnError {
			os.Exit(1)
		}
	}
	err = initStreams()
>>>>>>> 7a1d563 (gas tests)
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
	if publishers > 0 {
		err = startPublishers()
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
		logInfo("Declaring streams: %s", streams)
		env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetUri(
			rabbitmqBrokerUrl))
		if err != nil {
			logError("Error init stream connection: %s", err)
			return err
		}

		for _, streamName := range streams {

			err = env.DeclareStream(
				streamName,
				stream.NewStreamOptions().
					SetMaxLengthBytes(stream.ByteCapacity{}.From(maxLengthBytes)).
					SetMaxSegmentSizeBytes(stream.ByteCapacity{}.From(maxSegmentSizeBytes)))
			if err != nil {
				logError("Error declaring stream: %s", err)
				_ = env.Close()
				return err
			}
		}
		logInfo("End Init streams :%s\n", streams)
		return env.Close()
	}
	logInfo("Predeclared streams: %s\n", streams)
	return nil
}
func startPublishers() error {
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetUri(
		rabbitmqBrokerUrl).SetMaxProducersPerClient(publishersPerClient))
	if err != nil {
		logError("Error connection client publisher: %s", err)
		return err
	}
	logInfo("Starting %d publishers...", publishers)
	for _, streamName := range streams {
		for i := 1; i <= publishers; i++ {

			logInfo("Starting publisher number: %d", i)
			publisher, err := env.NewProducer(streamName,
				stream.NewProducerOptions().SetPublishConfirmHandler(func(ch <-chan []int64) {
					ids := <-ch
					atomic.AddInt32(&confirmedMessageCount, int32(len(ids)))
				}))
			if err != nil {
				logError("Error create publisher: %s", err)
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

					atomic.AddInt32(&publisherMessageCount, 100)
					_, err = prod.BatchPublish(context.Background(), arr)
					if err != nil {
						logError("Error publishing %s", err)
						time.Sleep(1 * time.Second)
					}
				}
			}(publisher, arr)
		}
	}
	return nil
}

func consumerCloseHandler(closeEvents <-chan stream.Event) {
	event := <-closeEvents
	fmt.Printf("close %s", event)
}

func startConsumer(consumerName string, streamName string) error {
	_, err := simulEnvironment.NewConsumer(context.TODO(), streamName, func(Context stream.ConsumerContext, message *amqp.Message) {
		if atomic.AddInt32(&consumerMessageCount, 1)%500 == 0 {
			err := Context.Consumer.Commit()
			if err != nil {
				logError("Error Commit: %s", err)
			}
		}
	}, consumerCloseHandler,
		stream.NewConsumerOptions().
		//SetOffset(stream.OffsetSpecification{}.First()).
		SetConsumerName(uuid.New().String()))

}

func startConsumers() error {
	logInfo("Starting %d consumers...", consumers)


	for _, streamName := range streams {
		for i := 0; i < consumers; i++ {
			randomSleep()
			logInfo("Starting consumer number: %d", i)
<<<<<<< HEAD
			_, err = env.NewConsumer(
				streamName,
				func(Context stream.ConsumerContext, message *amqp.Message) {
					if atomic.AddInt32(&consumerMessageCount, 1)%500 == 0 {
						err := Context.Consumer.Commit()
						if err != nil {
							logError("Error Commit: %s", err)
						}
					}
				},
				stream.NewConsumerOptions().SetConsumerName(fmt.Sprintf("%s-%d", streamName, i)))
=======
			_, err := simulEnvironment.NewConsumer(context.TODO(), streamName, func(Context stream.ConsumerContext, message *amqp.Message) {
				if atomic.AddInt32(&consumerMessageCount, 1)%500 == 0 {
					err := Context.Consumer.Commit()
					if err != nil {
						logError("Error Commit: %s", err)
					}
				}
			}, consumerCloseHandler, stream.NewConsumerOptions().
				SetOffset(stream.OffsetSpecification{}.First()).
				SetConsumerName(uuid.New().String()))
>>>>>>> 7a1d563 (gas tests)
			if err != nil {
				logError("Error creating consumer: %s", err)
				return err
			}

		}
	}
	return nil
}
