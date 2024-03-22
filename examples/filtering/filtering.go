package main

import (
	"bufio"
	"errors"
	"fmt"
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

	//You need RabbitMQ 3.13.0 or later to run this example
	fmt.Println("Filtering example.")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	// Connect to the broker ( or brokers )
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
	CheckErr(err)

	streamName := "FilteringExampleStream"
	err = env.DeleteStream(streamName)
	if err != nil && errors.Is(err, stream.StreamDoesNotExist) {
		// we can ignore the error if the stream does not exist
		// it will be created later
		fmt.Println("Stream does not exist. ")
	} else {
		CheckErr(err)
	}

	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)

	CheckErr(err)

	producer, err := env.NewProducer(streamName,
		stream.NewProducerOptions().SetFilter(
			// Here we enable the filter
			// for each message we set the filter key.
			// the filter result is a string
			stream.NewProducerFilter(func(message message.StreamMessage) string {
				return fmt.Sprintf("%s", message.GetApplicationProperties()["state"])
			})))
	CheckErr(err)

	chPublishConfirm := producer.NotifyPublishConfirmation()
	handlePublishConfirm(chPublishConfirm)

	// Send messages with the state property == New York
	send(producer, "New York")
	// Here we wait a bit to be sure that the messages are stored in the same chunk
	// and we can filter them
	// This is only for the example, in a real case you don't need to wait
	time.Sleep(2 * time.Second)

	// Send messages with the state property == Alabama
	send(producer, "Alabama")

	// Here we wait a bit to be sure that the messages are stored in another chunk
	// only for testing the filter
	time.Sleep(1 * time.Second)

	err = producer.Close()
	CheckErr(err)

	// post filter is applied client side after the server side filter
	// the server side filter is applied on the broker side
	// In real scenarios, the chunk could contain messages that do not match the filter
	// that's why we need to apply the filter client side
	// NOTE: This code _must_ be simple and fast. Don't introduce complex logic here with possible bugs
	// Post filter is mandatory as function even you can return always true
	postFilter := func(message *amqp.Message) bool {
		// you can use any amqp.Message field to filter
		// be sure the field is set ann valid before sending the message
		return message.ApplicationProperties["state"] == "New York"
	}

	// Here we create a consumer with a filter
	// the filter is applied server side
	// with "New York" as a filter
	filter := stream.NewConsumerFilter([]string{"New York"}, true, postFilter)

	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		// Here you should process received only messages that match the filter
		// "New York" messages should be received
		// "Alabama" messages should not be received
		fmt.Printf("consumer name: %s, data: %s, message offset %d, chunk entities count: %d   \n ",
			consumerContext.Consumer.GetName(), message.Data, consumerContext.Consumer.GetOffset(), consumerContext.GetEntriesCount())
	}

	consumer, err := env.NewConsumer(
		streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetOffset(stream.OffsetSpecification{}.First()). // start consuming from the beginning
			SetFilter(filter))                               // set the filter
	CheckErr(err)
	channelClose := consumer.NotifyClose()
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

func send(producer *stream.Producer, state string) {
	for i := 0; i < 100; i++ {
		msg := amqp.NewMessage([]byte(fmt.Sprintf("message %d, state %s", i, state)))
		msg.ApplicationProperties = map[string]interface{}{"state": state}
		err := producer.Send(msg)
		CheckErr(err)
	}
}
