package main

import (
	"bufio"
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

func CreateArrayMessagesForTesting(bacthMessages int) []*amqp.Message {
	var arr []*amqp.Message
	for z := 0; z < bacthMessages; z++ {
		arr = append(arr, amqp.NewMessage([]byte(uuid.NewString())))
	}
	return arr
}

func handlePublishConfirm(confirms stream.ChannelPublishConfirm) {
	go func() {
		for messagesIds := range confirms {
			for _, m := range messagesIds {
				fmt.Printf("Confirmed %s message \n  ", m.Message.Data)
			}
		}
	}()
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
	// Create a stream, you can create streams without any option like:
	// err = env.DeclareStream(streamName, nil)
	// it is a best practise to define a size,  1GB for example:

	streamName := uuid.New().String()
	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)

	//CheckErr(err)
	rProducer := stream.NewHAProducer(env)
	err = rProducer.NewProducer(streamName, "producer-ha")
	CheckErr(err)

	//optional publish confirmation channel
	chPublishConfirm := rProducer.NotifyPublishConfirmation()
	handlePublishConfirm(chPublishConfirm)

	// each publish sends a number of messages, the batchMessages should be around 100 messages for send
	for i := 0; i < 2000; i++ {
		err := rProducer.BatchPublish(CreateArrayMessagesForTesting(10))
		time.Sleep(10 * time.Millisecond)
		CheckErr(err)
	}

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	time.Sleep(200 * time.Millisecond)
	err = rProducer.Close()
	CheckErr(err)
	err = env.DeleteStream(streamName)
	CheckErr(err)
	err = env.Close()
	CheckErr(err)
}
