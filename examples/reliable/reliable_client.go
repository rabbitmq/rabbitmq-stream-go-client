package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

// The ha producer and consumer provide a way to auto-reconnect in case of connection problems

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

const enableResend = false

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
	go func() {
		//nolint:gosec
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// Tune the parameters to test the reliability
	const messagesToSend = 3_000_000
	const numberOfProducers = 1
	const concurrentProducers = 1
	const numberOfConsumers = 5
	const sendDelay = 100 * time.Microsecond
	const delayEachMessages = 500
	const maxProducersPerClient = 2
	const maxConsumersPerClient = 5
	//

	reader := bufio.NewReader(os.Stdin)
	stream.SetLevelInfo(logs.INFO)

	fmt.Printf("\n%s%s  RabbitMQ Stream  ·  Reliable Client%s\n\n", ansiBold, ansiMagenta, ansiReset)
	fmt.Printf("  Connecting to RabbitMQ streaming...\n\n")

	//  in case of load-balancer you can use the AddressResolver
	//  var resolver = stream.AddressResolver{
	//	Host: "localhost",
	//	Port: 5552,
	//}

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetMaxProducersPerClient(maxProducersPerClient).
			SetMaxConsumersPerClient(maxConsumersPerClient).
			SetHost("localhost").
			SetUser("guest").
			SetPassword("guest").
			SetPort(5552))
	//  SetHost(resolver.Host).
	//  SetPort(resolver.Port).
	//  SetAddressResolver(resolver))

	CheckErr(err)
	fmt.Printf("  %sConnected%s  (max %d producers / %d consumers per client)\n\n",
		ansiGreen, ansiReset, maxProducersPerClient, maxConsumersPerClient)
	producers := make([]*ha.ReliableProducer, 0, numberOfProducers)
	consumers := make([]*ha.ReliableConsumer, 0, numberOfConsumers)
	isRunning := true

	streamsName := []string{"golang-reliable-Test", "golang-reliable-Test-1", "golang-reliable-Test-2", "golang-reliable-Test-3"}
	for _, streamName := range streamsName {
		err = env.DeleteStream(streamName)
		// If the stream does not exist,
		// we don't care here as we are going to create it anyway
		if !errors.Is(err, stream.StreamDoesNotExist) {
			CheckErr(err)
		}
		err = env.DeclareStream(streamName,
			&stream.StreamOptions{
				MaxLengthBytes: stream.ByteCapacity{}.GB(10),
			},
		)
		CheckErr(err)

		go func() {
			for isRunning {
				totalConfirmed := atomic.LoadInt32(&confirmed) + atomic.LoadInt32(&fail)
				expectedMessages := int32((messagesToSend * numberOfProducers * concurrentProducers * 2) * len(streamsName))
				cfmd := atomic.LoadInt32(&confirmed)
				failed := atomic.LoadInt32(&fail)
				cons := atomic.LoadInt32(&consumed)

				var confirmRate float64
				if totalConfirmed > 0 {
					confirmRate = float64(cfmd) / float64(totalConfirmed) * 100
				}

				perConsumer := int32(0)
				if numberOfConsumers > 0 {
					perConsumer = cons / numberOfConsumers
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

				fmt.Println()
				sep()

				time.Sleep(5 * time.Second)
			}
		}()

		for range numberOfConsumers {
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

	fmt.Printf("\n  %sPress enter to close the connections.%s\n", ansiDim, ansiReset)
	_, _ = reader.ReadString('\n')
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
	isRunning = false
	fmt.Printf("  %sConnections closed.%s  Press enter to close the environment.\n", ansiGreen, ansiReset)
	_, _ = reader.ReadString('\n')

	err = env.Close()
	CheckErr(err)
}
