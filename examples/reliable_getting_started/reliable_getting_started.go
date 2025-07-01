package main

import (
	"errors"
	"fmt"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func main() {
	fmt.Printf("Getting started with Streaming client for RabbitMQ\n")

	// Create the environment. You can set the log level to DEBUG for more information
	// stream.SetLevelInfo(logs.DEBUG)
	// the environment is the connection to the broker(s)
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().
		SetHost("localhost").
		SetPort(5552))
	if err != nil {
		fmt.Printf("Error creating environment: %v\n", err)
		return
	}

	// Create a stream
	streamName := "my-stream"
	// It is highly recommended to define the stream retention policy
	err = env.DeclareStream(streamName, stream.NewStreamOptions().
		SetMaxLengthBytes(stream.ByteCapacity{}.GB(2)))

	// ignore the error if the stream already exists
	if err != nil && !errors.Is(err, stream.StreamAlreadyExists) {
		fmt.Printf("Error declaring stream: %v\n", err)
		return
	}

	// declare the reliable consumer using the package ha
	consumer, err := ha.NewReliableConsumer(env, streamName,
		// start from the beginning of the stream
		stream.NewConsumerOptions().
			SetOffset(stream.OffsetSpecification{}.First()),
		// handler where the messages will be processed
		func(_ stream.ConsumerContext, message *amqp.Message) {
			fmt.Printf("Message received: %s\n", message.GetData())
		})

	if err != nil {
		fmt.Printf("Error creating consumer: %v\n", err)
		return
	}

	// Create the reliable producer using the package ha
	producer, err := ha.NewReliableProducer(env, streamName,
		// we leave the default options
		stream.NewProducerOptions(),
		// handler for the confirmation of the messages
		func(messageConfirm []*stream.ConfirmationStatus) {
			for _, msg := range messageConfirm {
				if msg.IsConfirmed() {
					fmt.Printf("message %s confirmed \n", msg.GetMessage().GetData())
				} else {
					fmt.Printf("message %s failed \n", msg.GetMessage().GetData())
				}
			}
		})

	if err != nil {
		fmt.Printf("Error creating producer: %v\n", err)
		return
	}

	// Send a message
	for i := range 10 {
		err = producer.Send(amqp.NewMessage([]byte(fmt.Sprintf("Hello stream:%d", i))))
		if err != nil {
			fmt.Printf("Error sending message: %v\n", err)
			return
		}
	}

	// press any key to exit
	fmt.Printf("Press any close the producer, consumer and environment\n")
	_, _ = fmt.Scanln()

	//// Close the producer
	err = producer.Close()
	if err != nil {
		fmt.Printf("Error closing producer: %v\n", err)
	}

	// Close the consumer
	err = consumer.Close()
	if err != nil {
		fmt.Printf("Error closing consumer: %v\n", err)
	}

	err = env.DeleteStream(streamName)
	if err != nil {
		fmt.Printf("Error deleting stream: %v\n", err)
	}

	// Close the environment
	err = env.Close()
	if err != nil {
		fmt.Printf("Error closing environment: %s\n", err)
	}
}
