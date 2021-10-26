package main

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"os"
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
				if msg.Confirmed {
					atomic.AddInt32(&messagesConfirmed, 1)
				}
			}
		}
	}()
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("RabbitMQ Sub Entry Batch example")

	// Connect to the broker ( or brokers )
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
	CheckErr(err)
	fmt.Printf("------------------------------------------\n\n")
	fmt.Println("Connected to the RabbitMQ server")

	streamName := uuid.New().String()
	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)
	CheckErr(err)
	fmt.Printf("------------------------------------------\n\n")
	fmt.Printf("Created Stream: %s \n", streamName)

	producer, err := env.NewProducer(streamName, stream.NewProducerOptions().
		SetSubEntrySize(500).
		SetCompression(stream.Compression{}.None()))
	CheckErr(err)

	//optional publish confirmation channel
	chPublishConfirm := producer.NotifyPublishConfirmation()
	handlePublishConfirm(chPublishConfirm)
	messagesToSend := 20_000
	fmt.Printf("------------------------------------------\n\n")
	fmt.Printf("Start sending %d messages, data size: %d bytes\n", messagesToSend, len("hello_world"))
	batchSize := 100
	var arr []message.StreamMessage
	for i := 0; i < batchSize; i++ {
		arr = append(arr, amqp.NewMessage([]byte("hello_world")))
	}

	start := time.Now()
	for i := 0; i < messagesToSend; i++ {
		err := producer.BatchSend(arr)
		CheckErr(err)
	}
	duration := time.Since(start)
	fmt.Printf("------------------------------------------\n\n")
	fmt.Printf("Sent %d messages in %s \n", messagesToSend*100, duration)
	fmt.Printf("------------------------------------------\n\n")

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	time.Sleep(200 * time.Millisecond)
	CheckErr(err)
	err = env.DeleteStream(streamName)
	CheckErr(err)
	err = env.Close()
	CheckErr(err)
}
