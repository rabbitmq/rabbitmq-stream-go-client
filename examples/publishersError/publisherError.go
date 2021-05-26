package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
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

func CreateArrayMessagesForTesting(bacthMessages int) []*amqp.Message {
	var arr []*amqp.Message
	for z := 0; z < bacthMessages; z++ {
		arr = append(arr, amqp.NewMessage([]byte("hello_world_"+strconv.Itoa(z))))
	}
	return arr
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	// Set log level, not mandatory by default is INFO
	//stream.SetLevelInfo(stream.DEBUG)

	fmt.Println("Getting started with Streaming client for RabbitMQ")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	//chPublishError := make(chan stream.PublishError, 10)
	//go func(ch chan stream.PublishError) {
	//
	//	for {
	//		pError := <-ch
	//		atomic.AddInt32(&totalMessages, 1)
	//		fmt.Printf("Error during publish, message:%s ,  error: %s \n", pError.UnConfirmedMessage.UnConfirmedMessage.Data, pError.Err)
	//	}
	//
	//}(chPublishError)
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
	//SetPublishErrorListener(chPublishError))
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

	producer, err := env.NewProducer(streamName, &stream.ProducerOptions{Name: "myProducer"})
	CheckErr(err)

	chPublishError := producer.NotifyPublishError()
	handlePublishError(chPublishError)

	// each publish sends a number of messages, the batchMessages should be around 100 messages for send
	go func() {
		for i := 0; i < 100; i++ {
			_, err := producer.BatchPublish(context.Background(), CreateArrayMessagesForTesting(2))
			CheckErr(err)
			//time.Sleep(100 * time.Millisecond)
		}
	}()
	//time.Sleep(100 * time.Millisecond)
	err = producer.Close()
	CheckErr(err)

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	CheckErr(err)

}

func handlePublishError(publishError stream.ChannelPublishError) {
	go func() {
		var totalMessages int32
		for {
			pError := <-publishError
			atomic.AddInt32(&totalMessages, 1)
			var data [][]byte
			if pError.UnConfirmedMessage != nil {
				data = pError.UnConfirmedMessage.Message.Data
			}
			fmt.Printf("Error during publish, message:%s ,  error: %s. Total %d  \n", data, pError.Err, totalMessages)
		}
	}()

}
