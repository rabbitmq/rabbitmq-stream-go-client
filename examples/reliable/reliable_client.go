package main

import (
	"bufio"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

// The ha producer and consumer provide a way to auto-reconnect in case of connection problems
//
// All the parameters below can be overridden with environment variables so this example
// can be run as-is inside a container (see the Dockerfile in this directory).

const (
	ansiReset   = "\033[0m"
	ansiRed     = "\033[31m"
	ansiGreen   = "\033[32m"
	ansiYellow  = "\033[33m"
	ansiMagenta = "\033[35m"
	ansiCyan    = "\033[36m"
	ansiBold    = "\033[1m"
	ansiDim     = "\033[2m"
	clearScreen = "\033[2J\033[H"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s%s%s\n", ansiRed, err, ansiReset)
		os.Exit(1)
	}
}

var confirmed int32 = 0
var fail int32 = 0
var consumed int32 = 0
var sent int32
var reSent int32

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func getEnvInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return i
}

func getEnvStringSlice(key string, fallback []string) []string {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	parts := strings.Split(v, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			result = append(result, p)
		}
	}
	if len(result) == 0 {
		return fallback
	}
	return result
}

func formatCommas(num int32) string {
	str := fmt.Sprintf("%d", num)
	re := regexp.MustCompile(`(\d+)(\d{3})`)
	for n := ""; n != str; {
		n = str
		str = re.ReplaceAllString(str, "$1,$2")
	}
	return str
}

func colorStatus(s string) string {
	switch s {
	case "Open":
		return ansiGreen + s + ansiReset
	case "Closed":
		return ansiRed + s + ansiReset
	case "Reconnecting":
		return ansiYellow + s + ansiReset
	default:
		return ansiDim + s + ansiReset
	}
}

func progressBar(percent float64, width int) string {
	if percent < 0 {
		percent = 0
	}
	if percent > 100 {
		percent = 100
	}
	filled := int(percent / 100.0 * float64(width))
	empty := width - filled
	return ansiGreen + strings.Repeat("█", filled) + ansiDim + strings.Repeat("░", empty) + ansiReset
}

func sep() {
	fmt.Printf("%s%s%s\n", ansiDim, strings.Repeat("─", 66), ansiReset)
}

func sectionTitle(title string) {
	fmt.Printf("\n  %s%s%s\n", ansiBold+ansiCyan, title, ansiReset)
}

func main() {
	// Connection parameters
	rabbitmqHost := getEnv("RABBITMQ_HOST", "localhost")
	rabbitmqPort := getEnvInt("RABBITMQ_PORT", 5552)
	rabbitmqUser := getEnv("RABBITMQ_USER", "guest")
	rabbitmqPassword := getEnv("RABBITMQ_PASSWORD", "guest")
	rabbitmqVHost := getEnv("RABBITMQ_VHOST", "/")
	rabbitmqTLS := getEnvBool("RABBITMQ_TLS", false)
	rabbitmqTLSSkipVerify := getEnvBool("RABBITMQ_TLS_SKIP_VERIFY", true)

	// pprof, useful to check the resources used by the application in case of reconnection
	pprofEnabled := getEnvBool("PPROF_ENABLED", true)
	pprofAddr := getEnv("PPROF_ADDR", "localhost:6060")

	// Tune the parameters to test the reliability
	messagesToSend := getEnvInt("MESSAGES_TO_SEND", 1_000_000)
	numberOfProducers := getEnvInt("NUMBER_OF_PRODUCERS", 2)
	concurrentProducers := getEnvInt("CONCURRENT_PRODUCERS", 1)
	numberOfConsumers := getEnvInt("NUMBER_OF_CONSUMERS", 2)
	sendDelay := time.Duration(getEnvInt("SEND_DELAY_MICROS", 100)) * time.Microsecond
	delayEachMessages := getEnvInt("DELAY_EACH_MESSAGES", 500)
	maxProducersPerClient := getEnvInt("MAX_PRODUCERS_PER_CLIENT", 2)
	maxConsumersPerClient := getEnvInt("MAX_CONSUMERS_PER_CLIENT", 5)
	maxLengthBytesGB := int64(getEnvInt("MAX_STREAM_LENGTH_GB", 10))
	statsIntervalSeconds := getEnvInt("STATS_INTERVAL_SECONDS", 5)
	enableResend := getEnvBool("ENABLE_RESEND", false)
	// runs without waiting on stdin, and shuts down on SIGINT/SIGTERM - useful when running in a container
	silentMode := getEnvBool("IS_SILENT", false)

	// addSuperStream also exercises a super stream, in addition to the normal streams above
	addSuperStream := getEnvBool("ADD_SUPER_STREAM", true)
	numberOfPartitions := getEnvInt("NUMBER_OF_PARTITIONS", 3)
	superStreamName := getEnv("SUPER_STREAM_NAME", "golang-reliable-super-stream-Test")

	streamsName := getEnvStringSlice("STREAM_NAMES",
		[]string{"golang-reliable-Test", "golang-reliable-Test-1", "golang-reliable-Test-2"})

	go func() {
		if !pprofEnabled {
			return
		}
		//nolint:gosec
		log.Println(http.ListenAndServe(pprofAddr, nil))
	}()

	reader := bufio.NewReader(os.Stdin)
	stream.SetLevelInfo(logs.INFO)

	fmt.Printf("\n%s%s  RabbitMQ Stream  ·  Reliable Client%s\n\n", ansiBold, ansiMagenta, ansiReset)
	fmt.Printf("  Connecting to RabbitMQ streaming...\n\n")

	//  in case of load-balancer you can use the AddressResolver
	var resolver = stream.AddressResolver{
		Host: rabbitmqHost,
		Port: rabbitmqPort,
	}

	envOptions := stream.NewEnvironmentOptions().
		SetMaxProducersPerClient(maxProducersPerClient).
		SetMaxConsumersPerClient(maxConsumersPerClient).
		SetUser(rabbitmqUser).
		SetPassword(rabbitmqPassword).
		SetVHost(rabbitmqVHost).
		SetHost(resolver.Host).
		SetPort(resolver.Port).
		SetAddressResolver(resolver)

	if rabbitmqTLS {
		envOptions.IsTLS(true).SetTLSConfig(&tls.Config{InsecureSkipVerify: rabbitmqTLSSkipVerify}) //nolint:gosec
	}

	env, err := stream.NewEnvironment(envOptions)

	CheckErr(err)
	fmt.Printf("  %sConnected%s  (max %d producers / %d consumers per client)\n\n",
		ansiGreen, ansiReset, maxProducersPerClient, maxConsumersPerClient)
	producers := make([]*ha.ReliableProducer, 0, numberOfProducers)
	consumers := make([]*ha.ReliableConsumer, 0, numberOfConsumers)
	superProducers := make([]*ha.ReliableSuperStreamProducer, 0, numberOfProducers)
	superConsumers := make([]*ha.ReliableSuperStreamConsumer, 0, numberOfConsumers)
	isRunning := true

	for _, streamName := range streamsName {
		err = env.DeleteStream(streamName)
		// If the stream does not exist,
		// we don't care here as we are going to create it anyway
		if !errors.Is(err, stream.StreamDoesNotExist) {
			CheckErr(err)
		}
		err = env.DeclareStream(streamName,
			&stream.StreamOptions{
				MaxLengthBytes: stream.ByteCapacity{}.GB(maxLengthBytesGB),
			},
		)
		CheckErr(err)

		go func() {
			for isRunning {
				totalConfirmed := atomic.LoadInt32(&confirmed) + atomic.LoadInt32(&fail)
				expectedMessages := int32((messagesToSend * numberOfProducers * concurrentProducers * 2) * len(streamsName))
				if addSuperStream {
					expectedMessages += int32(messagesToSend * numberOfProducers * concurrentProducers)
				}
				cfmd := atomic.LoadInt32(&confirmed)
				failed := atomic.LoadInt32(&fail)
				cons := atomic.LoadInt32(&consumed)

				var confirmRate float64
				if totalConfirmed > 0 {
					confirmRate = float64(cfmd) / float64(totalConfirmed) * 100
				}

				perConsumer := int32(0)
				if numberOfConsumers > 0 {
					perConsumer = cons / int32(numberOfConsumers)
				}

				fmt.Print(clearScreen)

				sep()
				fmt.Printf("  %s%s  RabbitMQ Stream  ·  Reliable Client  %s\n", ansiBold, ansiMagenta, ansiReset)
				sep()

				fmt.Printf("\n  %-12s%s%s%s\n", "Streams", ansiBold, streamsName, ansiReset)
				fmt.Printf("  %-12s%s\n", "Time", time.Now().Format("Mon, 02 Jan 06 15:04:05 MST"))

				sectionTitle("CONFIGURATION")
				fmt.Printf("  Producers: %s%d%s   Concurrent: %s%d%s   Consumers: %s%d%s   Goroutines: %s%d%s\n",
					ansiBold, numberOfProducers, ansiReset,
					ansiBold, concurrentProducers, ansiReset,
					ansiBold, numberOfConsumers, ansiReset,
					ansiBold, runtime.NumGoroutine(), ansiReset,
				)

				sectionTitle("MESSAGES")
				fmt.Printf("  Target  %s%-14s%s  Sent  %s%-14s%s  ReSent  %s%s%s\n",
					ansiYellow, formatCommas(expectedMessages), ansiReset,
					ansiGreen, formatCommas(sent), ansiReset,
					ansiCyan, formatCommas(atomic.LoadInt32(&reSent)), ansiReset,
				)

				sectionTitle("CONFIRMATIONS")
				fmt.Printf("  Confirmed  %s%-12s%s  Failed  %s%-12s%s  Total  %s%s%s\n",
					ansiGreen, formatCommas(cfmd), ansiReset,
					ansiRed, formatCommas(failed), ansiReset,
					ansiDim, formatCommas(totalConfirmed), ansiReset,
				)
				fmt.Printf("  Rate  %s%.1f%%%s  %s\n",
					ansiBold, confirmRate, ansiReset, progressBar(confirmRate, 30))

				sectionTitle("CONSUMPTION")
				fmt.Printf("  Total  %s%-14s%s  Per Consumer  %s%s%s\n",
					ansiGreen, formatCommas(cons), ansiReset,
					ansiBold, formatCommas(perConsumer), ansiReset,
				)

				sectionTitle(fmt.Sprintf("PRODUCERS (%d)", len(producers)))
				for i, producer := range producers {
					fmt.Printf("  [%d] %-40s  %s\n", i+1, producer.GetInfo(), colorStatus(producer.GetStatusAsString()))
				}

				sectionTitle(fmt.Sprintf("CONSUMERS (%d)", len(consumers)))
				for i, consumer := range consumers {
					fmt.Printf("  [%d] %-40s  %s\n", i+1, consumer.GetInfo(), colorStatus(consumer.GetStatusAsString()))
				}

				if addSuperStream {
					sectionTitle(fmt.Sprintf("SUPER STREAM PRODUCERS (%d)", len(superProducers)))
					for i, producer := range superProducers {
						fmt.Printf("  [%d] %-40s  %s\n", i+1, producer.GetStreamName(), colorStatus(producer.GetStatusAsString()))
					}

					sectionTitle(fmt.Sprintf("SUPER STREAM CONSUMERS (%d)", len(superConsumers)))
					for i, consumer := range superConsumers {
						fmt.Printf("  [%d] %-40s  %s\n", i+1, consumer.GetStreamName(), colorStatus(consumer.GetStatusAsString()))
					}
				}

				fmt.Println()
				sep()

				time.Sleep(time.Duration(statsIntervalSeconds) * time.Second)
			}
		}()

		for i := 0; i < numberOfConsumers; i++ {
			consumer, err := ha.NewReliableConsumer(env,
				streamName,
				stream.NewConsumerOptions().SetOffset(stream.OffsetSpecification{}.First()),
				func(ctx stream.ConsumerContext, _ *amqp.Message) {
					atomic.AddInt32(&consumed, 1)
					if ctx.Consumer.GetStreamName() != streamName {
						panic(fmt.Sprintf("Received message for stream %s on consumer for stream %s", ctx.Consumer.GetStreamName(), streamName))
					}
				})
			CheckErr(err)
			consumers = append(consumers, consumer)
		}

		for i := 0; i < numberOfProducers; i++ {
			var mutex = sync.Mutex{}
			// Here we store the messages that have not been confirmed
			// then we resend them.
			// Note: This is only for test. The list can grow indefinitely
			var unConfirmedMessages []message.StreamMessage
			rProducer, err := ha.NewReliableProducer(env,
				streamName,
				stream.NewProducerOptions().
					SetConfirmationTimeOut(2*time.Second).
					SetClientProvidedName(fmt.Sprintf("producer-%d", i)),
				func(messageStatus []*stream.ConfirmationStatus) {
					go func() {
						for _, msgStatus := range messageStatus {
							if msgStatus.IsConfirmed() {
								atomic.AddInt32(&confirmed, 1)
							} else {
								atomic.AddInt32(&fail, 1)
								if enableResend {
									mutex.Lock()
									unConfirmedMessages = append(unConfirmedMessages, msgStatus.GetMessage())
									mutex.Unlock()
								}
							}
						}
					}()
				})
			CheckErr(err)
			producers = append(producers, rProducer)
			go func() {
				for i := 0; i < concurrentProducers; i++ {
					go func() {
						for i := 0; i < messagesToSend; i++ {
							mutex.Lock()
							for _, confirmedMessage := range unConfirmedMessages {
								err := rProducer.Send(confirmedMessage)
								atomic.AddInt32(&reSent, 1)
								CheckErr(err)
							}
							unConfirmedMessages = []message.StreamMessage{}
							mutex.Unlock()
							msg := amqp.NewMessage([]byte("ha"))
							err := rProducer.Send(msg)
							if i%delayEachMessages == 0 {
								time.Sleep(sendDelay)
							}
							atomic.AddInt32(&sent, 1)
							CheckErr(err)

							errBatch := rProducer.BatchSend([]message.StreamMessage{msg})
							CheckErr(errBatch)
							atomic.AddInt32(&sent, 1)
						}
					}()
				}
			}()
		}
	}

	if addSuperStream {
		err = env.DeleteSuperStream(superStreamName)
		if !errors.Is(err, stream.StreamDoesNotExist) {
			CheckErr(err)
		}
		err = env.DeclareSuperStream(superStreamName,
			stream.NewPartitionsOptions(numberOfPartitions).
				SetMaxLengthBytes(stream.ByteCapacity{}.GB(maxLengthBytesGB)))
		CheckErr(err)

		for i := 0; i < numberOfConsumers; i++ {
			superConsumer, err := ha.NewReliableSuperStreamConsumer(env,
				superStreamName,
				func(_ stream.ConsumerContext, _ *amqp.Message) {
					atomic.AddInt32(&consumed, 1)
				},
				stream.NewSuperStreamConsumerOptions().SetOffset(stream.OffsetSpecification{}.First()))
			CheckErr(err)
			superConsumers = append(superConsumers, superConsumer)
		}

		for i := 0; i < numberOfProducers; i++ {
			superProducer, err := ha.NewReliableSuperStreamProducer(env,
				superStreamName,
				stream.NewSuperStreamProducerOptions(stream.NewHashRoutingStrategy(func(msg message.StreamMessage) string {
					return msg.GetMessageProperties().MessageID.(string)
				})).SetClientProvidedName(fmt.Sprintf("super-producer-%d", i)),
				func(messageConfirm []*stream.PartitionPublishConfirm) {
					go func() {
						for _, partitionConfirm := range messageConfirm {
							for _, msgStatus := range partitionConfirm.ConfirmationStatus {
								if msgStatus.IsConfirmed() {
									atomic.AddInt32(&confirmed, 1)
								} else {
									atomic.AddInt32(&fail, 1)
								}
							}
						}
					}()
				})
			CheckErr(err)
			superProducers = append(superProducers, superProducer)
			go func() {
				for i := 0; i < concurrentProducers; i++ {
					go func() {
						for i := 0; i < messagesToSend; i++ {
							msg := amqp.NewMessage([]byte("ha-super-stream"))
							msg.Properties = &amqp.MessageProperties{
								MessageID: fmt.Sprintf("super-%d-%d", i, time.Now().UnixNano()),
							}
							err := superProducer.Send(msg)
							if i%delayEachMessages == 0 {
								time.Sleep(sendDelay)
							}
							atomic.AddInt32(&sent, 1)
							CheckErr(err)
						}
					}()
				}
			}()
		}
	}

	if silentMode {
		fmt.Printf("\n  %sRunning in silent mode.%s  Send SIGINT/SIGTERM to close the connections.\n", ansiDim, ansiReset)
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
	} else {
		fmt.Printf("\n  %sPress enter to close the connections.%s\n", ansiDim, ansiReset)
		_, _ = reader.ReadString('\n')
	}
	for _, producer := range producers {
		err := producer.Close()
		if err != nil {
			CheckErr(err)
		}
	}
	for _, consumer := range consumers {
		err := consumer.Close()
		if err != nil {
			CheckErr(err)
		}
	}
	if addSuperStream {
		for _, producer := range superProducers {
			err := producer.Close()
			if err != nil {
				CheckErr(err)
			}
		}
		for _, consumer := range superConsumers {
			err := consumer.Close()
			if err != nil {
				CheckErr(err)
			}
		}
	}
	isRunning = false
	if !silentMode {
		fmt.Printf("  %sConnections closed.%s  Press enter to close the environment.\n", ansiGreen, ansiReset)
		_, _ = reader.ReadString('\n')
	}

	err = env.Close()
	CheckErr(err)
}
