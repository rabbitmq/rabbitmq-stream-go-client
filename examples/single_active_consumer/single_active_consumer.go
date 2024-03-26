package main

import (
	"bufio"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"os"
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

	//You need RabbitMQ 3.11.0 or later to run this example
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
		fmt.Printf("[ %s ] - consumer name: %s, data: %s, message offset %d,   \n ", appName,
			consumerContext.Consumer.GetName(), message.Data, consumerContext.Consumer.GetOffset())
	}

	consumer, err := env.NewConsumer(
		streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetOffset(stream.OffsetSpecification{}.First()).
			SetConsumerName(consumerName).
			SetSingleActiveConsumer(
				stream.NewSingleActiveConsumer(func(isActive bool) stream.OffsetSpecification {
					fmt.Printf("Consumer promoted. Active status: %t\n", isActive)
					return stream.OffsetSpecification{}.First()
				})))
	CheckErrConsumer(err)

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	err = consumer.Close()
	CheckErrConsumer(err)
	err = env.Close()
	CheckErrConsumer(err)

}
