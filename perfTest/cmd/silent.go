package cmd

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/spf13/cobra"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var wg sync.WaitGroup

func newSilent() *cobra.Command {
	var silentCmd = &cobra.Command{
		Use:   "silent",
		Short: "NewProducer a silent simulation",
		RunE: func(cmd *cobra.Command, args []string) error {
			wg.Add(1)
			err := startSimulation()
			if err == nil {
				wg.Wait()
			}
			return err
		},
	}
	return silentCmd
}

var (
	publisherMessageCount    int32
	consumerMessageCount     int32
	confirmedMessageCount    int32
	notConfirmedMessageCount int32
	consumersCloseCount      int32
	publishErrors            int32
	messagesSent             int64
	//connections           []*stream.Client
	simulEnvironment *stream.Environment
)

func printStats() {
	if printStatsV {
		start := time.Now()
		ticker := time.NewTicker(1 * time.Second)
		go func() {
			for {
				select {
				case _ = <-ticker.C:
					v := time.Now().Sub(start).Milliseconds()

					PMessagesPerSecond := float64(atomic.LoadInt32(&publisherMessageCount)) / float64(v) * 1000
					CMessagesPerSecond := float64(atomic.LoadInt32(&consumerMessageCount)) / float64(v) * 1000
					ConfirmedMessagesPerSecond := float64(atomic.LoadInt32(&confirmedMessageCount)) / float64(v) * 1000

					logInfo("Published %8.2f msg/s   |   Confirmed %8.2f msg/s   |   Consumed %8.2f msg/s   |  Cons. closed %3v  |   Pub errors %3v  |   %3v  |  %3v  |  msg sent: %3v  |",
						PMessagesPerSecond, ConfirmedMessagesPerSecond, CMessagesPerSecond, consumersCloseCount, publishErrors, decodeRate(), decodeBody(), atomic.LoadInt64(&messagesSent))
				}
			}

		}()
		tickerReset := time.NewTicker(1 * time.Minute)
		go func() {
			for {
				select {
				case _ = <-tickerReset.C:
					start = time.Now()

					atomic.SwapInt32(&publisherMessageCount, 0)
					atomic.SwapInt32(&consumerMessageCount, 0)
					atomic.SwapInt32(&confirmedMessageCount, 0)
					atomic.SwapInt32(&notConfirmedMessageCount, 0)
				}
			}

		}()
	}
}

func decodeBody() string {
	if publishers > 0 {
		if fixedBody > 0 {
			return fmt.Sprintf("Fixed Body: %d", fixedBody)
		}
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
	if debugLogs {
		stream.SetLevelInfo(logs.DEBUG)
	}

	if batchSize < 1 || batchSize > 200 {
		logError("Invalid batchSize, must be from 1 to 200, value:%d", batchSize)
		os.Exit(1)
	}

	if rate > 0 && rate < batchSize {
		batchSize = rate
		logInfo("Rate lower than batch size, move batch size: %d", batchSize)
	}

	logInfo("Silent (%s) Simulation, url: %s publishers: %d consumers: %d streams: %s ", stream.ClientVersion, rabbitmqBrokerUrl, publishers, consumers, streams)

	err := initStreams()
	checkErr(err)

	simulEnvironment, err = stream.NewEnvironment(stream.NewEnvironmentOptions().
		SetUris(rabbitmqBrokerUrl).
		SetMaxProducersPerClient(publishersPerClient).
		SetMaxConsumersPerClient(consumersPerClient))
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
		logError("error: %s", err)
		if exitOnError {
			os.Exit(1)
		}
	}
}

func randomSleep() {
	rand.Seed(time.Now().UnixNano())
	n := rand.Intn(500)
	time.Sleep(time.Duration(n) * time.Millisecond)
}

func initStreams() error {
	logInfo("Declaring streams: %s", streams)
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetUris(
		rabbitmqBrokerUrl).SetAddressResolver(stream.AddressResolver{
		Host: rabbitmqBrokerUrl[0],
		Port: 5552,
	}))
	if err != nil {
		logError("Error init stream connection: %s", err)
		return err
	}

	for _, streamName := range streams {
		err = env.DeclareStream(
			streamName,
			stream.NewStreamOptions().
				SetMaxAge(time.Duration(maxAge)*time.Hour).
				SetMaxLengthBytes(stream.ByteCapacity{}.From(maxLengthBytes)).
				SetMaxSegmentSizeBytes(stream.ByteCapacity{}.From(maxSegmentSizeBytes)))
		if err != nil {
			if err == stream.PreconditionFailed {
				logError("The stream: %s already exist with different parameters", streamName)
				_ = env.Close()
				return err
			}

			if err != stream.StreamAlreadyExists {
				logError("Error during stream %s creation, err: %s", streamName, err)
				_ = env.Close()
				return err
			}
		}

		streamMetadata, err := env.StreamMetaData(streamName)
		checkErr(err)
		logInfo("stream %s, meta data: %s", streamName, streamMetadata)

	}
	logInfo("End Init streams :%s\n", streams)
	return env.Close()
}

func handlePublishConfirms(confirms stream.ChannelPublishConfirm) {
	go func() {
		for ids := range confirms {
			for _, msg := range ids {
				if msg.Confirmed {
					atomic.AddInt32(&confirmedMessageCount, 1)
				} else {
					atomic.AddInt32(&notConfirmedMessageCount, 1)
				}

			}

		}
	}()
}

func handlePublishError(publishError stream.ChannelPublishError) {
	go func() {
		for range publishError {
			atomic.AddInt32(&publishErrors, 1)
		}
	}()

}

func startPublisher(streamName string) error {

	rPublisher, err := ha.NewHAProducer(simulEnvironment, streamName, nil)
	if err != nil {
		logError("Error create publisher: %s", err)
		return err
	}

	chPublishConfirm := rPublisher.NotifyPublishConfirmation()
	handlePublishConfirms(chPublishConfirm)

	chPublishError := rPublisher.NotifyPublishError()
	handlePublishError(chPublishError)

	var arr []message.StreamMessage
	var body string
	for z := 0; z < batchSize; z++ {
		body = fmt.Sprintf("1234567890")

		if fixedBody > 0 {
			body = ""
			for i := 0; i < fixedBody; i++ {
				body += "s"
			}
		} else {
			if variableBody > 0 {
				body = ""
				rand.Seed(time.Now().UnixNano())
				n := rand.Intn(variableBody)
				for i := 0; i < n; i++ {
					body += "s"
				}
			}
		}

		arr = append(arr, amqp.NewMessage([]byte(body)))
	}

	go func(prod *ha.ReliableProducer, messages []message.StreamMessage) {
		for {
			if rate > 0 {
				rateWithBatchSize := float64(rate) / float64(batchSize)
				sleepAfterMessage := float64(time.Second) / rateWithBatchSize
				time.Sleep(time.Duration(sleepAfterMessage))

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
			atomic.AddInt32(&publisherMessageCount, int32(len(arr)))

			for _, streamMessage := range arr {
				atomic.AddInt64(&messagesSent, 1)
				err = prod.Send(streamMessage)
				if err != nil {
					logError("Error publishing: %s", err)
				}
			}
			checkErr(err)

		}
	}(rPublisher, arr)

	return nil

}

func startPublishers() error {

	logInfo("Starting %d publishers...", publishers)

	for _, streamName := range streams {
		for i := 1; i <= publishers; i++ {
			logInfo("Starting publisher number: %d", i)
			err := startPublisher(streamName)
			checkErr(err)
		}
	}
	return nil
}

func handleConsumerClose(channelClose stream.ChannelClose) {
	go func() {
		event := <-channelClose
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

}
func startConsumer(consumerName string, streamName string) error {

	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		//logError("consumerMessageCount StoreOffset: %s", consumerMessageCount)
		atomic.AddInt32(&consumerMessageCount, 1)

	}
	offsetSpec := stream.OffsetSpecification{}.Last()
	switch consumerOffest {
	case "last":
		offsetSpec = stream.OffsetSpecification{}.Last()
	case "first":
		offsetSpec = stream.OffsetSpecification{}.First()
	case "next":
		offsetSpec = stream.OffsetSpecification{}.Next()
	}

	logInfo("Starting consumer number: %s, form %s", consumerName, offsetSpec)

	consumer, err := simulEnvironment.NewConsumer(
		streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetConsumerName(consumerName).SetOffset(offsetSpec))
	if err != nil {
		return err
	}
	chConsumerClose := consumer.NotifyClose()
	handleConsumerClose(chConsumerClose)

	return nil
}

func startConsumers() error {
	logInfo("Starting %d consumers...", consumers)

	for _, streamName := range streams {
		for i := 0; i < consumers; i++ {
			randomSleep()
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
