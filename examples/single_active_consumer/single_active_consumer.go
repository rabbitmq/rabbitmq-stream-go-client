package main

import (
	"bufio"
	"fmt"
	"os"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func CheckErrConsumer(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}
func main() {
	if len(os.Args) != 2 {
		fmt.Printf("You need to specify the Name\n")
		os.Exit(1)
	}
	appName := os.Args[1]
	reader := bufio.NewReader(os.Stdin)

	// You need RabbitMQ 3.11.0 or later to run this example
	fmt.Println("Single Active Consumer example.")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	// Connect to the broker ( or brokers )
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
	CheckErrConsumer(err)

	streamName := "SingleActiveConsumer"
	// you need to set the same name.
	// The name indicates the group of consumers
	// to make the single active consumer work
	consumerName := "MyApplication"

	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		fmt.Printf("[%s] - [ %s ] - consumer name: %s, data: %s, message offset %d,   \n ",
			time.Now().Format(time.TimeOnly),
			appName,
			consumerContext.Consumer.GetName(), message.Data, consumerContext.Consumer.GetOffset())
		// This is only for the example, in a real application you should not store the offset
		// for each message, it is better to store the offset for a batch of messages
		err := consumerContext.Consumer.StoreOffset()

		CheckErrConsumer(err)
	}

	consumerUpdate := func(streamName string, isActive bool) stream.OffsetSpecification {
		// This function is called when the consumer is promoted to active
		// be careful with the logic here, it is called in the consumer thread
		// the code here should be fast, non-blocking and without side effects
		fmt.Printf("[%s] - Consumer promoted for: %s. Active status: %t\n", time.Now().Format(time.TimeOnly),
			streamName, isActive)

		// In this example, we store the offset server side and we retrieve it
		// when the consumer is promoted to active
		offset, err := env.QueryOffset(consumerName, streamName)
		if err != nil {
			// If the offset is not found, we start from the beginning
			return stream.OffsetSpecification{}.First()
		}

		// If the offset is found, we start from the last offset
		// we add 1 to the offset to start from the next message
		return stream.OffsetSpecification{}.Offset(offset + 1)
	}

	consumer, err := env.NewConsumer(
		streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetConsumerName(consumerName).
			// It is not needed to set the SetOffset() when the SingleActiveConsumer is active
			// the `consumerUpdate` function replaces it
			SetSingleActiveConsumer(
				stream.NewSingleActiveConsumer(consumerUpdate)))

	CheckErrConsumer(err)

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	err = consumer.Close()
	CheckErrConsumer(err)
	fmt.Printf("[%s] Consumer stopped.... in 5 seconds the environment will be closed", time.Now().Format(time.TimeOnly))
	time.Sleep(5 * time.Second)
	err = env.Close()
	CheckErrConsumer(err)
}
