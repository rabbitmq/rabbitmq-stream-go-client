package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"os"
	"time"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}

func CreateArrayMessagesForTesting(batchMessages int) []*amqp.Message {
	var arr []*amqp.Message
	for z := 0; z < batchMessages; z++ {
		arr = append(arr, amqp.NewMessage([]byte("1234567890")))
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
			SetPassword("guest").
			SetMaxConsumersPerClient(1))
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

	producer, err := env.NewProducer(streamName, nil)
	CheckErr(err)

	go func() {
		for i := 0; i < 2; i++ {
			_, err = producer.BatchPublish(context.Background(), CreateArrayMessagesForTesting(100))
			time.Sleep(1 * time.Second)
		}
	}()

	counter := 0
	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		counter = counter + 1
		fmt.Printf("messages consumed: %d \n ", counter)
	}

	consumer, err := env.NewConsumer(context.TODO(), streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetConsumerName("my_consumer"). // set a consumer name
			SetOffset(stream.OffsetSpecification{}.Offset(100))) // start specific offset, in this case we start from the 100 so it will consume 100 messages
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
