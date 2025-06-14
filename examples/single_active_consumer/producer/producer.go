package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
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
					fmt.Printf("[%s] message %s stored \n", time.Now().Format(time.TimeOnly), msg.GetMessage().GetData())
				} else {
					fmt.Printf("[%s] message %s failed \n", time.Now().Format(time.TimeOnly), msg.GetMessage().GetData())
				}
			}
		}
	}()
}

func main() {
	fmt.Println("Producer for Single Active Consumer example")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	// Connect to the broker ( or brokers )
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
	CheckErr(err)
	streamName := "SingleActiveConsumer"
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
	producer, err := env.NewProducer(streamName, nil)
	CheckErr(err)

	// optional publish confirmation channel
	chPublishConfirm := producer.NotifyPublishConfirmation()
	handlePublishConfirm(chPublishConfirm)

	// Put some sleep to make the example easy
	for i := range 10000 {
		var body = fmt.Sprintf("hello_world_%d", i)
		fmt.Printf("[%s] sending message %s ...\n", time.Now().Format(time.TimeOnly), body)
		err := producer.Send(amqp.NewMessage([]byte(body)))
		CheckErr(err)
		time.Sleep(3 * time.Second)
	}

	err = producer.Close()
	CheckErr(err)

	err = env.Close()
	CheckErr(err)
}
