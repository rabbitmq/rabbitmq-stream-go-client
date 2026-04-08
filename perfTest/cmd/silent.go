package cmd

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/spf13/cobra"
	"golang.org/x/text/language"
	gomsg "golang.org/x/text/message"
)

var randomSource = rand.New(rand.NewSource(time.Now().UnixNano()))
var wg sync.WaitGroup

func newSilent() *cobra.Command {
	var silentCmd = &cobra.Command{
		Use:   "silent",
		Short: "Start the performance test (default command)",

		RunE: func(_ *cobra.Command, _ []string) error {
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
	publisherMessageCount int32
	consumerMessageCount  int32
	// consumerMessageCountPerLatency int32
	totalLatency             int64
	confirmedMessageCount    int32
	notConfirmedMessageCount int32
	consumersCloseCount      int32
	// connections           []*stream.Client
	simulEnvironment *stream.Environment
)

func addressResolverFromURI(uri string) (stream.AddressResolver, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return stream.AddressResolver{}, err
	}
	host := u.Hostname()
	if host == "" {
		return stream.AddressResolver{}, fmt.Errorf("no host in URI")
	}
	port, err := strconv.Atoi(stream.StreamTcpPort)
	if err != nil {
		return stream.AddressResolver{}, err
	}
	if p := u.Port(); p != "" {
		port, err = strconv.Atoi(p)
		if err != nil {
			return stream.AddressResolver{}, fmt.Errorf("invalid port in URI: %w", err)
		}
	}
	return stream.AddressResolver{Host: host, Port: port}, nil
}

func checkRunDuration() {
	if runDuration > 0 {
		start := time.Now()
		ticker := time.NewTicker(10 * time.Second)
		go func() {
			for range ticker.C {
				v := time.Since(start).Seconds()
				if v >= float64(runDuration) {
					logInfo("%s stopping after %ds", style(ansiBold+ansiYellow, "time"), runDuration)
					os.Exit(0)
				}
			}
		}()
	}
}

func printStats() {
	if printStatsV {
		start := time.Now()
		ticker := time.NewTicker(1 * time.Second)
		go func() {
			for range ticker.C {
				v := time.Since(start).Milliseconds()
				PMessagesPerSecond := float64(0)
				if publisherMessageCount > 0 {
					PMessagesPerSecond = float64(atomic.LoadInt32(&publisherMessageCount)) / float64(v) * 1000
				}

				averageLatency := int64(0)
				CMessagesPerSecond := float64(0)
				if atomic.LoadInt32(&consumerMessageCount) > 0 {
					CMessagesPerSecond = float64(atomic.LoadInt32(&consumerMessageCount)) / float64(v) * 1000
					averageLatency = totalLatency / int64(atomic.LoadInt32(&consumerMessageCount))
				}

				ConfirmedMessagesPerSecond := float64(0)
				if atomic.LoadInt32(&confirmedMessageCount) > 0 {
					ConfirmedMessagesPerSecond = float64(atomic.LoadInt32(&confirmedMessageCount)) / float64(v) * 1000
				}
				p := gomsg.NewPrinter(language.English)
				pub := p.Sprintf("%8.1f", PMessagesPerSecond)
				conf := p.Sprintf("%8.1f", ConfirmedMessagesPerSecond)
				cons := p.Sprintf("%8.1f", CMessagesPerSecond)
				rateStr := decodeRate()
				bodyStr := decodeBody()
				lat := p.Sprintf("%d", averageLatency)
				line := fmt.Sprintf(
					"%s %s msg/s | %s %s msg/s | %s %s msg/s |  %s | %s | %s %s ms",
					style(ansiBold+ansiCyan, "publish"),
					style(ansiBold+ansiCyan, pub),
					style(ansiBold+ansiGreen, "confirm"),
					style(ansiBold+ansiGreen, conf),
					style(ansiBold+ansiMagenta, "consume"),
					style(ansiBold+ansiMagenta, cons),
					style(ansiDim+ansiHiGreen, rateStr),
					style(ansiDim+ansiHiGreen, bodyStr),
					style(ansiBold+ansiYellow, "latency"),
					style(ansiBold+ansiYellow, lat),
				)
				logInfo("%s", line)
			}
		}()

		tickerReset := time.NewTicker(1 * time.Minute)
		go func() {
			for range tickerReset.C {
				logInfo("%s", style(ansiBold+ansiYellow, "---------- resetting throughput counters ----------"))
				atomic.SwapInt32(&consumerMessageCount, 0)
				atomic.SwapInt32(&notConfirmedMessageCount, 0)
				atomic.SwapInt32(&confirmedMessageCount, 0)
				atomic.SwapInt32(&publisherMessageCount, 0)
				atomic.SwapInt64(&totalLatency, 0)
				start = time.Now()
			}
		}()
	}
}

func decodeBody() string {
	if publishers > 0 {
		if fixedBody > 0 {
			return fmt.Sprintf("Body sz: %d", fixedBody+8)
		}
		if variableBody > 0 {
			return fmt.Sprintf("Body vsz: %d", variableBody)
		}
		return fmt.Sprintf("Body sz: %d", 8)
	} else {
		return "ND"
	}
}

func decodeRate() string {
	if publishers > 0 {
		if rate > 0 {
			return fmt.Sprintf("Rate Fx: %d", rate)
		}
		if variableRate > 0 {
			return fmt.Sprintf("Rate Vr: %d", variableRate)
		}
		return "Full"
	} else {
		return "ND"
	}
}

func startSimulation() error {
	if debugLogs {
		stream.SetLevelInfo(logs.DEBUG)
	}

	if batchSize < 1 || batchSize > 300 {
		logError("Invalid batchSize, must be from 1 to 300, value:%d", batchSize)
		os.Exit(1)
	}

	if rate > 0 && rate < batchSize {
		batchSize = rate
		logInfo("%s rate < batch size; batch size set to %d", opTag("publish", ansiCyan), batchSize)
	}

	logInfo("%s Silent %s | brokers: %v | publishers: %d | consumers: %d | streams: %v",
		style(ansiBold+ansiHiCyan, ">>"),
		style(ansiBold+ansiWhite, stream.ClientVersion),
		rabbitmqBrokerUrl, publishers, consumers, streams)

	err := initStreams()
	checkErr(err)

	envOpts := stream.NewEnvironmentOptions().
		SetUri(rabbitmqBrokerUrl[0]).
		SetMaxProducersPerClient(publishersPerClient).
		SetMaxConsumersPerClient(consumersPerClient)
	if loadBalancer {
		resolver, resErr := addressResolverFromURI(rabbitmqBrokerUrl[0])
		if resErr != nil {
			logError("load-balancer: %s", resErr)
			os.Exit(1)
		}
		envOpts = envOpts.SetAddressResolver(resolver)
	}
	simulEnvironment, err = stream.NewEnvironment(envOpts)
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
	checkRunDuration()

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
	n := randomSource.Intn(500)
	time.Sleep(time.Duration(n) * time.Millisecond)
}

func initStreams() error {
	logInfo("%s Declaring streams: %v", opTag("stream", ansiBlue), streams)
	initOpts := stream.NewEnvironmentOptions().SetUris(rabbitmqBrokerUrl)
	if loadBalancer {
		resolver, resErr := addressResolverFromURI(rabbitmqBrokerUrl[0])
		if resErr != nil {
			logError("load-balancer: %s", resErr)
			return resErr
		}
		initOpts = initOpts.SetAddressResolver(resolver)
	}
	env, err := stream.NewEnvironment(initOpts)
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
		logInfo("%s stream %s meta: %s", opTag("stream", ansiBlue), streamName, streamMetadata)
	}
	logInfo("%s Streams ready: %v", opTag("stream", ansiBlue), streams)
	return env.Close()
}

func handlePublishConfirms(messageConfirm []*stream.ConfirmationStatus) {
	go func() {
		for _, msg := range messageConfirm {
			if msg.IsConfirmed() {
				atomic.AddInt32(&confirmedMessageCount, 1)
			} else {
				atomic.AddInt32(&notConfirmedMessageCount, 1)
			}
		}
	}()
}

func startPublisher(streamName string) error {
	producerOptions := stream.NewProducerOptions()

	if subEntrySize > 1 {
		cp := stream.Compression{}.None()
		if compression == "gzip" {
			cp = stream.Compression{}.Gzip()
		}

		if compression == "lz4" {
			cp = stream.Compression{}.Lz4()
		}
		if compression == "snappy" {
			cp = stream.Compression{}.Snappy()
		}
		if compression == "zstd" {
			cp = stream.Compression{}.Zstd()
		}
		producerOptions.SetSubEntrySize(subEntrySize).SetCompression(cp)
		logInfo("%s sub-entry batch: size=%d compression=%s", opTag("batch", ansiYellow), subEntrySize, cp)
	}

	producerOptions.SetClientProvidedName(clientProvidedName).SetBatchSize(batchSize)
	rPublisher, err := ha.NewReliableProducer(simulEnvironment,
		streamName,
		producerOptions,
		handlePublishConfirms)
	if err != nil {
		logError("Error create publisher: %s", err)
		return err
	}

	go func(prod *ha.ReliableProducer) {
		for {
			if rate > 0 {
				rateWithBatchSize := float64(rate) / float64(batchSize)
				sleepAfterMessage := float64(time.Second) / rateWithBatchSize
				time.Sleep(time.Duration(sleepAfterMessage))
			}

			if variableRate > 0 {
				n := randomSource.Intn(variableRate)
				sleep := float64(batchSize) / float64(n)

				sleep *= 1000
				if sleep > 3000 {
					sleep = 0
				}
				time.Sleep(time.Duration(sleep) * time.Millisecond)
			}
			messages := buildMessages()

			if isAsyncSend {
				for _, streamMessage := range messages {
					err = prod.Send(streamMessage)
					checkErr(err)
				}
			} else {
				err = prod.BatchSend(messages)
				checkErr(err)
			}
			atomic.AddInt32(&publisherMessageCount, int32(len(messages)))
		}
	}(rPublisher)

	return nil
}

func buildMessages() []message.StreamMessage {
	arr := make([]message.StreamMessage, 0, batchSize)
	for range batchSize {
		var body []byte
		if fixedBody > 0 {
			body = make([]byte, fixedBody)
		} else if variableBody > 0 {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			body = make([]byte, r.Intn(variableBody))
		}
		buf := make([]byte, 8, 8+len(body))
		sentTime := time.Now().UnixMilli()
		binary.BigEndian.PutUint64(buf, uint64(sentTime))
		/// added to calculate the latency
		msg := amqp.NewMessage(append(buf, body...))
		arr = append(arr, msg)
	}
	return arr
}

func startPublishers() error {
	logInfo("%s Starting %d publisher(s)...", opTag("publish", ansiCyan), publishers)

	for _, streamName := range streams {
		for i := 1; i <= publishers; i++ {
			logInfo("%s publisher #%d on stream %s", opTag("publish", ansiCyan), i, streamName)
			err := startPublisher(streamName)
			checkErr(err)
		}
	}
	return nil
}

func handleConsumerClose(channelClose stream.ChannelClose) {
	go func() {
		event := <-channelClose
		logInfo("%s consumer %s closed on %s: %s", opTag("consume", ansiMagenta), event.Name, event.StreamName, event.Reason)
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
	handleMessages := func(_ stream.ConsumerContext, message *amqp.Message) {
		sentTime := binary.BigEndian.Uint64(message.GetData()[:8]) // Decode the timestamp
		startTimeFromMessage := time.UnixMilli(int64(sentTime))
		latency := time.Since(startTimeFromMessage).Milliseconds()
		totalLatency += latency
		atomic.AddInt32(&consumerMessageCount, 1)
	}
	offsetSpec := stream.OffsetSpecification{}.Last()
	switch consumerOffset {
	case "last":
		offsetSpec = stream.OffsetSpecification{}.Last()
	case "first":
		offsetSpec = stream.OffsetSpecification{}.First()
	case "next":
		offsetSpec = stream.OffsetSpecification{}.Next()
	case "random":
		n := randomSource.Intn(3)
		switch n {
		case 0:
			offsetSpec = stream.OffsetSpecification{}.First()
		case 1:
			offsetSpec = stream.OffsetSpecification{}.Next()
		case 2:
			offsetSpec = stream.OffsetSpecification{}.Last()
		}
	}

	logInfo("%s consumer %s offset %s", opTag("consume", ansiMagenta), consumerName, offsetSpec)

	consumer, err := simulEnvironment.NewConsumer(
		streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetConsumerName(consumerName).
			SetClientProvidedName(clientProvidedName).
			SetOffset(offsetSpec).
			SetCRCCheck(crcCheck).
			SetInitialCredits(int16(initialCredits)))
	if err != nil {
		return err
	}
	chConsumerClose := consumer.NotifyClose()
	handleConsumerClose(chConsumerClose)

	return nil
}

func startConsumers() error {
	logInfo("%s Starting %d consumer(s)...", opTag("consume", ansiMagenta), consumers)

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
