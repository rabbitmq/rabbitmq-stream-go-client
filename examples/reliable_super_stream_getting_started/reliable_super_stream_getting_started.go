package main

import (
	"errors"
	"fmt"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func main() {
	fmt.Printf("Getting started with Super Stream client for RabbitMQ \n")

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

	// Create a super stream
	streamName := "my-super-stream"
	// It is highly recommended to define the stream retention policy
	err = env.DeclareSuperStream(streamName, stream.NewPartitionsOptions(1).
		SetMaxLengthBytes(stream.ByteCapacity{}.GB(2)))

	// ignore the error if the stream already exists
	if err != nil && !errors.Is(err, stream.StreamAlreadyExists) {
		fmt.Printf("Error declaring stream: %v\n", err)
		return
	}

	// declare the reliable consumer using the package ha
	consumer, err := ha.NewReliableSuperStreamConsumer(env, streamName,
		// handler where the messages will be processed
		func(_ stream.ConsumerContext, message *amqp.Message) {
			fmt.Printf("Message received: %s\n", message.GetData())
		},
		// start from the beginning of the stream
		stream.NewSuperStreamConsumerOptions().SetOffset(stream.OffsetSpecification{}.First()),
	)

	if err != nil {
		fmt.Printf("Error creating consumer: %v\n", err)
		return
	}

	// Create the reliable producer using the package ha
	producer, err := ha.NewReliableSuperStreamProducer(env, streamName,
		// we leave the default options
		stream.NewSuperStreamProducerOptions(stream.NewHashRoutingStrategy(func(message message.StreamMessage) string {
			return message.GetMessageProperties().MessageID.(string)
		})),
		// handler for the confirmation of the messages
		func(messageConfirm []*stream.PartitionPublishConfirm) {
			for _, msgs := range messageConfirm {
				for _, msg := range msgs.ConfirmationStatus {
					if msg.IsConfirmed() {
						fmt.Printf("message %s confirmed \n", msg.GetMessage().GetData())
					} else {
						fmt.Printf("message %s failed \n", msg.GetMessage().GetData())
					}
				}
			}
		})

	if err != nil {
		fmt.Printf("Error creating producer: %v\n", err)
		return
	}

	// Send a messages
	for i := range 10000 {
		msg := amqp.NewMessage([]byte(fmt.Sprintf("Hello stream:%d", i)))
		msg.Properties = &amqp.MessageProperties{
			MessageID: fmt.Sprintf("msg-%d", i),
		}
		err = producer.Send(msg)
		if err != nil {
			fmt.Printf("Error sending message: %v\n", err)
			time.Sleep(1 * time.Second)
		}
		if i%1000 == 0 {
			fmt.Printf("Sent %d messages\n", i)
		}
		// add a small delay in case you want to kill connection to see the reliable reconnection in action
		time.Sleep(200 * time.Millisecond)
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

	err = env.DeleteSuperStream(streamName)
	if err != nil {
		fmt.Printf("Error deleting stream: %v\n", err)
	}

	// Close the environment
	err = env.Close()
	if err != nil {
		fmt.Printf("Error closing environment: %s\n", err)
	}
}
