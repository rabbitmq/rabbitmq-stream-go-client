package main

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}

var messagesConfirmed int32

func handlePublishConfirm(confirms stream.ChannelPublishConfirm) {
	go func() {
		for confirmed := range confirms {
			for _, msg := range confirmed {
				if msg.IsConfirmed() {
					atomic.AddInt32(&messagesConfirmed, 1)
				}
			}
		}
	}()
}

func main() {

	useSyncBatch := os.Args[1] == "sync"
	useAsyncSend := os.Args[1] == "async"

	messagesToSend, err := strconv.Atoi(os.Args[2])
	CheckErr(err)
	batchSize, err := strconv.Atoi(os.Args[3])
	messagesToSend = messagesToSend / batchSize

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("RabbitMQ performance example")

	// Connect to the broker ( or brokers )
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
	CheckErr(err)
	fmt.Println("Connected to the RabbitMQ server")

	streamName := uuid.New().String()
	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)
	CheckErr(err)
	fmt.Printf("Created Stream: %s \n", streamName)

	producer, err := env.NewProducer(streamName,
		stream.NewProducerOptions().
			SetBatchSize(batchSize).
			SetBatchPublishingDelay(100))
	CheckErr(err)

	chPublishConfirm := producer.NotifyPublishConfirmation()
	handlePublishConfirm(chPublishConfirm)
	fmt.Printf("------------------------------------------\n\n")
	fmt.Printf("Start sending %d messages, data size: %d bytes\n", messagesToSend*batchSize, len("hello_world"))
	var averageLatency time.Duration
	var messagesConsumed int32
	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		atomic.AddInt32(&messagesConsumed, 1)
		var latency time.Time
		err := latency.UnmarshalBinary(message.Data[0])
		CheckErr(err)
		averageLatency += time.Since(latency)
	}
	_, err = env.NewConsumer(streamName, handleMessages, stream.NewConsumerOptions().SetOffset(stream.OffsetSpecification{}.First()))
	CheckErr(err)

	start := time.Now()

	// here the client sends the messages in batch and it is up to the user to aggregate the messages
	if useSyncBatch {
		var arr []message.StreamMessage
		for i := 0; i < messagesToSend; i++ {
			for i := 0; i < batchSize; i++ {
				latency, err := time.Now().MarshalBinary()
				CheckErr(err)
				arr = append(arr, amqp.NewMessage(latency))
			}
			err := producer.BatchSend(arr)
			CheckErr(err)
			arr = arr[:0]
		}
	}

	// here the client aggregates the messages based on the batch size and batch publishing delay
	if useAsyncSend {
		for i := 0; i < messagesToSend; i++ {
			for i := 0; i < batchSize; i++ {
				latency, err := time.Now().MarshalBinary()
				CheckErr(err)
				err = producer.Send(amqp.NewMessage(latency))
				CheckErr(err)
			}
		}
	}

	duration := time.Since(start)
	fmt.Println("Press any key to report and stop ")
	_, _ = reader.ReadString('\n')
	fmt.Printf("------------------------------------------\n\n")
	fmt.Printf("Sent %d messages in %s. Confirmed: %d avarage latency: %s \n", messagesToSend*batchSize, duration, messagesConfirmed, averageLatency/time.Duration(messagesConsumed))
	fmt.Printf("------------------------------------------\n\n")

	time.Sleep(200 * time.Millisecond)
	CheckErr(err)
	err = env.DeleteStream(streamName)
	CheckErr(err)
	err = env.Close()
	CheckErr(err)
}
