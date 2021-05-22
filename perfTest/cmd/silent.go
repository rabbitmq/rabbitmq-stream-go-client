package cmd

import (
	"context"
	"fmt"
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
	consumersCloseCount   int32
	publishErrors         int32
	//connections           []*stream.Client
	simulEnvironment *stream.Environment
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
					PMessagesPerSecond := int64(float64(atomic.LoadInt32(&publisherMessageCount)) / v)
					CMessagesPerSecond := int64(float64(atomic.LoadInt32(&consumerMessageCount)) / v)
					ConfirmedMessagesPerSecond := int64(float64(atomic.LoadInt32(&confirmedMessageCount)) / v)
					logInfo("Published %8v msg/s   |   Confirmed %8v msg/s   |   Consumed %6v msg/s   |  Cons. closed %3v  |   Pub errors %3v  |   %3v  |  %3v  |",
						PMessagesPerSecond, ConfirmedMessagesPerSecond, CMessagesPerSecond, consumersCloseCount, publishErrors, decodeRate(), decodeBody())
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
	if batchSize < 1 || batchSize > 200 {
		logError("Invalid batchSize, must be from 1 to 200, value:%d", batchSize)
		os.Exit(1)
	}

	logInfo("Silent (%s) Simulation, url: %s publishers: %d consumers: %d streams: %s ", stream.ClientVersion, rabbitmqBrokerUrl, publishers, consumers, streams)

	err := initStreams()
	checkErr(err)

	chPublishError := make(chan stream.PublishError, 1)
	go func(ch chan stream.PublishError) {
		for {
			pError := <-ch
			logError("publish %s error", pError.Name)
			atomic.AddInt32(&publishErrors, 1)

		}
	}(chPublishError)

	simulEnvironment, err = stream.NewEnvironment(stream.NewEnvironmentOptions().
		SetUri(rabbitmqBrokerUrl).
		SetMaxProducersPerClient(publishersPerClient).
		SetMaxConsumersPerClient(consumersPerClient).
		SetPublishErrorListener(chPublishError))
	checkErr(err)
	if consumers > 0 {
		err = startConsumers()
		checkErr(err)
	}
	if publishers > 0 {
		err = startPublishers()
		checkErr(err)
	}
	printStats()

	return err
}

func checkErr(err error) {
	if err != nil {
		if exitOnError {
			os.Exit(1)
		}
	}
}

func randomSleep() {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(500) // n will be between 0 and 2
	time.Sleep(time.Duration(n) * time.Millisecond)
}

func initStreams() error {
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
			if err == stream.PreconditionFailed {
				logError("The stream: %s already exist with different parameters", streamName)
				_ = env.Close()
				return err
			}
		}
	}
	logInfo("End Init streams :%s\n", streams)
	return env.Close()
}
func startPublishers() error {
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetUri(
		rabbitmqBrokerUrl).SetMaxProducersPerClient(publishersPerClient))
	if err != nil {
		logError("Error connection client publisher: %s", err)
		return err
	}
	logInfo("Starting %d publishers...", publishers)

	chPublishConfirm := make(chan []int64, 1)
	go func(ch chan []int64) {
		for {
			ids := <-ch
			//confirmedMessageCount += int32(len(ids))
			atomic.AddInt32(&confirmedMessageCount, int32(len(ids)))
		}
	}(chPublishConfirm)

	for _, streamName := range streams {
		for i := 1; i <= publishers; i++ {
			logInfo("Starting publisher number: %d", i)
			publisher, err := env.NewProducer(streamName, chPublishConfirm,
				stream.NewProducerOptions().SetProducerName(fmt.Sprintf("pub-%s-%d", streamName, i)))

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
						var v1 float64
						v1 = float64(rate) / float64(batchSize)

						sleep := float64(100) / v1
						sleep = sleep * 10
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

					atomic.AddInt32(&publisherMessageCount, int32(batchSize))
					_, err = prod.BatchPublish(context.Background(), arr)
					if err != nil {
						logError("Error publishing %s", err)
						time.Sleep(1 * time.Second)
					}
					checkErr(err)

				}
			}(publisher, arr)
		}
	}
	return nil
}

func startConsumer(consumerName string, streamName string) error {

	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		//logError("consumerMessageCount Commit: %s", consumerMessageCount)
		if atomic.AddInt32(&consumerMessageCount, 1)%500 == 0 {
			err := consumerContext.Consumer.Commit()
			if err != nil {
				logError("Error Commit: %s", err)
			}
		}
	}

	chConsumerClose := make(chan stream.Event, 0)
	go func() {
		event := <-chConsumerClose
		logInfo("Consumer %s closed on stream %s, cause: %s", event.Name, event.StreamName, event.Reason)
		if exitOnError {
			os.Exit(1)
		}
		atomic.AddInt32(&consumersCloseCount, 1)
		time.Sleep(200 * time.Millisecond)
		err := startConsumer(event.Name, event.StreamName)
		if err != nil {
			logError("Error starting consumer: %s", err)
		}
		checkErr(err)
	}()

	_, err := simulEnvironment.NewConsumer(context.TODO(),
		streamName,
		handleMessages,
		chConsumerClose,
		stream.NewConsumerOptions().
			SetConsumerName(consumerName))
	return err
}

func startConsumers() error {
	logInfo("Starting %d consumers...", consumers)

	for _, streamName := range streams {
		for i := 0; i < consumers; i++ {
			randomSleep()
			logInfo("Starting consumer number: %d", i)
			err := startConsumer(fmt.Sprintf("%s-%d", streamName, i), streamName)
			if err != nil {
				logError("Error creating consumer: %s", err)
				return err
			}
			checkErr(err)

		}
	}
	return nil
}
