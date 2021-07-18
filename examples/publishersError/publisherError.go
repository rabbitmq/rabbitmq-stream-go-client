package main

import (
	"bufio"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"

	//"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"os"
	"strconv"
	"sync/atomic"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Send Error example")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("test").
			SetPassword("test"))
	CheckErr(err)
	streamName := "pub-error"
	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)

	CheckErr(err)

	producer, err := env.NewProducer(streamName, stream.NewProducerOptions().SetProducerName("myProducer"))
	CheckErr(err)

	// This channel receives the callback in case the is some error during the
	// publisher.
	chPublishError := producer.NotifyPublishError()
	handlePublishError(chPublishError)

	for i := 0; i < 100; i++ {
		msg := amqp.NewMessage([]byte("hello_world_" + strconv.Itoa(i)))
		err := producer.Send(msg)
		CheckErr(err)
	}

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	CheckErr(err)
	err = env.DeleteStream(streamName)
	CheckErr(err)
	err = env.Close()
	CheckErr(err)

}

func handlePublishError(publishError stream.ChannelPublishError) {
	go func() {
		var totalMessages int32
		for pError := range publishError {
			atomic.AddInt32(&totalMessages, 1)
			var data [][]byte
			if pError.UnConfirmedMessage != nil {
				data = pError.UnConfirmedMessage.Message.GetData()
			}
			fmt.Printf("Error during publish, message:%s ,  error: %s. Total %d  \n", data, pError.Err, totalMessages)
		}
	}()

}
