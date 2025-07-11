package main

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}

func handlePublishConfirm(confirms stream.ChannelPublishConfirm) {
	go func() {
		for confirmed := range confirms {
			for _, msg := range confirmed {
				if msg.IsConfirmed() {
					fmt.Printf("message %s stored \n  ", msg.GetMessage().GetData())
				} else {
					fmt.Printf("message %s failed \n  ", msg.GetMessage().GetData())
				}
			}
		}
	}()
}

func consumerClose(channelClose stream.ChannelClose) {
	event := <-channelClose
	fmt.Printf("Consumer: %s closed on the stream: %s, reason: %s \n", event.Name, event.StreamName, event.Reason)
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	// Set log level, not mandatory by default is INFO
	stream.SetLevelInfo(logs.DEBUG)

	fmt.Println("Getting started with Streaming TLS client for RabbitMQ")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	// Connect to the broker ( or brokers )
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5551). // standard TLS port
			SetUser("guest").
			SetPassword("guest").
			IsTLS(true).
			// use tls.Config  to customize the TLS configuration
			// for tests you may need InsecureSkipVerify: true
			//nolint:gosec
			SetTLSConfig(&tls.Config{InsecureSkipVerify: true}),
	)
	/// TLS connection
	// it is also possible to configure the TLS connection using the URI
	// env, err := stream.NewEnvironment(
	//	stream.NewEnvironmentOptions().
	//		SetUri("rabbitmq-stream+tls://guest:guest@localhost:5551/").
	//		SetTLSConfig(&tls.Config{}),
	//)

	CheckErr(err)
	// Create a stream, you can create streams without any option like:
	// err = env.DeclareStream(streamName, nil)
	// it is the best practise to define a size,  1GB for example:

	streamName := uuid.New().String()
	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)

	CheckErr(err)

	// Get a new producer for a stream
	producer, err := env.NewProducer(streamName, nil)
	CheckErr(err)

	// optional publish confirmation channel
	chPublishConfirm := producer.NotifyPublishConfirmation()
	handlePublishConfirm(chPublishConfirm)

	// the send method automatically aggregates the messages
	// based on batch size
	for i := range 1000 {
		err := producer.Send(amqp.NewMessage([]byte("hello_world_" + strconv.Itoa(i))))
		CheckErr(err)
	}

	// this sleep is not mandatory, just to show the confirmed messages
	time.Sleep(1 * time.Second)
	err = producer.Close()
	CheckErr(err)

	// Define a consumer per stream, there are different offset options to define a consumer, default is
	// env.NewConsumer(streamName, func(Context streaming.ConsumerContext, message *amqp.message) {
	//
	// }, nil)
	// if you need to track the offset you need a consumer name like:
	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		fmt.Printf("consumer name: %s, text: %s \n ", consumerContext.Consumer.GetName(), message.Data)
	}

	consumer, err := env.NewConsumer(
		streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetConsumerName("my_consumer").                  // set a consumer name
			SetOffset(stream.OffsetSpecification{}.First())) // start consuming from the beginning
	CheckErr(err)
	channelClose := consumer.NotifyClose()
	// channelClose receives all the closing events, here you can handle the
	// client reconnection or just log
	defer consumerClose(channelClose)

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	err = consumer.Close()
	time.Sleep(200 * time.Millisecond)
	CheckErr(err)
	err = env.DeleteStream(streamName)
	CheckErr(err)
	err = env.Close()
	CheckErr(err)
}
