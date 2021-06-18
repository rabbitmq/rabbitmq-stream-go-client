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

func CreateArrayMessagesForTesting(bacthMessages int) []message.StreamMessage {
	var arr []message.StreamMessage
	for z := 0; z < bacthMessages; z++ {
		arr = append(arr, amqp.NewMessage([]byte("hello_world_"+strconv.Itoa(z))))
	}
	return arr
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	// Set log level, not mandatory by default is INFO
	//stream.SetLevelInfo(stream.DEBUG)

	fmt.Println("Getting started with Streaming client for RabbitMQ")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
	CheckErr(err)

	streamName := uuid.New().String()
	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.MB(500),
		},
	)
	CheckErr(err)

	producer, err := env.NewProducer(streamName, nil)
	CheckErr(err)

	// each publish sends a number of messages, the batchMessages should be around 100 messages for send
	go func() {
		for i := 0; i < 100; i++ {
			_, err := producer.BatchPublish(CreateArrayMessagesForTesting(100))
			CheckErr(err)
			time.Sleep(100 * time.Millisecond)
		}
	}()

	var count int32

	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		if atomic.AddInt32(&count, 1)%1000 == 0 {
			fmt.Printf("cousumed %d  messages \n", atomic.LoadInt32(&count))
			// AVOID to store for each single message, it will reduce the performances
			err := consumerContext.Consumer.StoreOffset()
			if err != nil {
				CheckErr(err)
			}
		}

	}

	consumer, err := env.NewConsumer(
		streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetConsumerName(uuid.New().String()).            // set a consumer name
			SetOffset(stream.OffsetSpecification{}.First())) // start consuming from the beginning
	CheckErr(err)

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	err = producer.Close()
	CheckErr(err)
	err = consumer.Close()
	CheckErr(err)
	err = env.DeleteStream(streamName)
	CheckErr(err)

}
