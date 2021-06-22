package main

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
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

func CreateArrayMessagesForTesting(batchMessages int) []message.StreamMessage {
	var arr []message.StreamMessage
	for z := 0; z < batchMessages; z++ {
		arr = append(arr, amqp.NewMessage([]byte("1234567890")))
	}
	return arr
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Start Offset example")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest").
			SetMaxConsumersPerClient(1))
	CheckErr(err)
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
			_, err = producer.BatchPublish(CreateArrayMessagesForTesting(100))
			time.Sleep(1 * time.Second)
		}
	}()

	counter := 0
	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		counter = counter + 1
		fmt.Printf("messages consumed: %d \n ", counter)
	}

	consumer, err := env.NewConsumer(streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetConsumerName("my_consumer"). // set a consumer name
			SetOffset(stream.OffsetSpecification{}.Offset(100)))
	// start specific offset, in this case we start from the 100 so it will consume 100 messages
	// see the others stream.OffsetSpecification{}.XXX
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
