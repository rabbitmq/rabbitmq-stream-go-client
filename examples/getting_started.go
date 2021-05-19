package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"os"
	"strconv"
	"time"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}

func CreateArrayMessagesForTesting(bacthMessages int) []*amqp.Message {
	var arr []*amqp.Message
	for z := 0; z < bacthMessages; z++ {
		arr = append(arr, amqp.NewMessage([]byte("hello_world_"+strconv.Itoa(z))))
	}
	return arr
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	// Set log level, not mandatory by default is INFO
	stream.SetLevelInfo(stream.DEBUG)

	fmt.Println("Getting started with Streaming client for RabbitMQ")
	fmt.Println("Connecting to RabbitMQ streaming ...")
	//uri := "rabbitmq-streaming://guest:guest@localhost:5551/%2f"
	// The environment is a wrapper around the TCP client connections

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5551).
			SetUser("guest").
			SetPassword("guest"))
	CheckErr(err)
	// Create a stream, you can create streams without any option like:
	// err = env.DeclareStream(streamName, nil)
	// it is a best practise to define a size,  1GB for example:
	streamName := uuid.New().String()
	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)

	CheckErr(err)

	//optional publish confirmation channel
	chPublishConfirm := make(chan []int64, 1)
	go func(ch chan []int64) {
		messagesIds := <-ch
		fmt.Printf("Confirmed %d messages \n \n ", len(messagesIds))
	}(chPublishConfirm)

	producer, err := env.NewProducer(streamName, chPublishConfirm, nil, nil)
	CheckErr(err)

	// each publish sends a number of messages, the batchMessages should be around 100 messages for send
	for i := 0; i < 2; i++ {
		_, err := producer.BatchPublish(context.Background(), CreateArrayMessagesForTesting(10))
		CheckErr(err)
	}

	// this sleep is not mandatory, just to show the confirmed messages
	time.Sleep(1 * time.Second)
	err = producer.Close()
	CheckErr(err)

	// Define a consumer per stream, there are different offset options to define a consumer, default is
	//env.NewConsumer(streamName, func(Context streaming.ConsumerContext, message *amqp.Message) {
	//
	//}, nil)
	// if you need to track the offset you need a consumer name like:
	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		fmt.Printf("consumer id: %d, text: %s \n ", consumerContext.Consumer.ID, message.Data)
		err := consumerContext.Consumer.Commit()
		if err != nil {
			fmt.Printf("Error during commit")
		}
	}

	// channelClose receives all the closing events, here you can handle the
	// client reconnection or just log
	channelClose := make(chan stream.Event, 1)
	go func() {
		event := <-channelClose
		fmt.Printf("Consumer: %s closed on the stream: %s, reason: %s \n", event.Name, event.StreamName, event.Reason)
	}()

	consumer, err := env.NewConsumer(context.TODO(), streamName,
		handleMessages,
		channelClose,
		stream.NewConsumerOptions().
			SetConsumerName("my_consumer").                  // set a consumer name
			SetOffset(stream.OffsetSpecification{}.First())) // start consuming from the beginning
	CheckErr(err)

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	err = consumer.UnSubscribe()
	time.Sleep(200 * time.Millisecond)
	CheckErr(err)
	err = env.Close()
	CheckErr(err)
}
