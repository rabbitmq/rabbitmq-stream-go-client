package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"os"
	"strconv"
	"time"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}

var idx = 0

func CreateArrayMessagesForTesting(bacthMessages int) []*amqp.Message {
	var arr []*amqp.Message
	for z := 0; z < bacthMessages; z++ {
		idx++
		arr = append(arr, amqp.NewMessage([]byte(strconv.Itoa(idx))))
	}
	return arr
}

func handlePublishConfirm(confirms stream.ChannelPublishConfirm) {
	go func() {
		for messagesIds := range confirms {
			for _, m := range messagesIds {
				if !m.Confirmed {
					fmt.Printf("Confirmed %s message - status %t \n  ", m.Message.Data, m.Confirmed)
				}
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

	streamName := "test"
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
	time.Sleep(4 * time.Second)
	// each publish sends a number of messages, the batchMessages should be around 100 messages for send
	for i := 0; i < 300; i++ {

		err := rProducer.BatchPublish(CreateArrayMessagesForTesting(10))
		time.Sleep(30 * time.Millisecond)
		CheckErr(err)
	}

	fmt.Println("Press any key to start consuming ")
	_, _ = reader.ReadString('\n')

	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		fmt.Printf("messages consumed: %s \n ", message.Data)
	}

	consumer, err := env.NewConsumer(context.TODO(), streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetConsumerName("my_consumer"))
	CheckErr(err)

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	time.Sleep(200 * time.Millisecond)
	err = rProducer.Close()
	CheckErr(err)
	err = consumer.Close()
	CheckErr(err)
	err = env.DeleteStream(streamName)
	CheckErr(err)
	err = env.Close()
	CheckErr(err)
}
