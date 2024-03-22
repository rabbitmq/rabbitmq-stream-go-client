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

	fmt.Println("Filtering example")
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

	// Get a new producer for a stream
	producer, err := env.NewProducer(streamName,
		stream.NewProducerOptions().SetFilter(
			stream.NewProducerFilter(func(message message.StreamMessage) string {
				return fmt.Sprintf("%s", message.GetApplicationProperties()["state"])
			})))
	CheckErr(err)

	//optional publish confirmation channel
	chPublishConfirm := producer.NotifyPublishConfirmation()
	handlePublishConfirm(chPublishConfirm)

	send(producer, "New York")
	// Here we wait a bit to be sure that the messages are stored in the same chunk
	// and we can filter them
	// This is only for the example, in a real case you don't need to wait
	time.Sleep(2 * time.Second)

	send(producer, "Alabama")

	time.Sleep(2 * time.Second)

	err = producer.Close()
	CheckErr(err)

	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {

		fmt.Printf("consumer name: %s, data: %s, message offset %d, chunk entities count: %d   \n ",
			consumerContext.Consumer.GetName(), message.Data, consumerContext.Consumer.GetOffset(), consumerContext.GetEntriesCount())
	}

	postFilter := func(message message.StreamMessage) bool {
		return true
	}

	filter := stream.NewConsumerFilter([]string{"New York"}, true, postFilter)

	consumer, err := env.NewConsumer(
		streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetClientProvidedName("my_consumer").            // connection name
			SetConsumerName("my_consumer").                  // set a consumer name
			SetOffset(stream.OffsetSpecification{}.First()). // start consuming from the beginning
			SetFilter(filter))
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

func send(producer *stream.Producer, state string) {
	for i := 0; i < 100; i++ {
		msg := amqp.NewMessage([]byte(fmt.Sprintf("message %d, state %s", i, state)))
		msg.ApplicationProperties = map[string]interface{}{"state": state}
		err := producer.Send(msg)
		CheckErr(err)
	}
}
