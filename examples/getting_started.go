package main

import (
	"bufio"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/gsantomaggio/go-stream-client/pkg/streaming"
	"os"
	"sync/atomic"
	"time"
)

func main() {
	fmt.Println("Getting started with Streaming client for RabbitMQ")
	fmt.Println("Connecting to RabbitMQ streaming ...")
	uris := "rabbitmq-streaming://guest:guest@localhost:5551/%2f"
	client, err := streaming.NewClientCreator().Uri(uris).Connect() // create Client Struct
	streaming.CheckErr(err)

	fmt.Println("Connected to localhost")
	streamName := "OffsetTest"
	err = client.StreamCreator().Stream(streamName).MaxAge(120 * time.Hour).Create() // Create the streaming queue

	var count int32

	streaming.CheckErr(err)
	consumer, err := client.ConsumerCreator().
		Stream(streamName).
		Name("my_consumer").
		Offset(streaming.OffsetSpecification{}.Offset(1500)).
		//Offset(streaming.OffsetSpecification{}.First()).
		MessagesHandler(func(consumerId uint8, message *amqp.Message) {
			atomic.AddInt32(&count, 1)
			//fmt.Printf("received %d, message %s total %d\n", consumerId, message.Data, count)
			fmt.Printf("received %d\n", count)
		}).Build()
	streaming.CheckErr(err)
	// Get a new producer to publish the messages
	producer, err := client.ProducerCreator().Stream(streamName).Build()
	streaming.CheckErr(err)
	numberOfMessages := 0
	batchSize := 100

	// Create AMQP 1.0 messages, see:https://github.com/Azure/go-amqp
	// message aggregation

	start := time.Now()
	for z := 0; z < numberOfMessages; z++ {
		var arr []*amqp.Message
		for f := 0; f < batchSize; f++ {
			arr = append(arr, amqp.NewMessage([]byte(fmt.Sprintf("test_%d_%d", z, f) )))
		}
		_, err = producer.BatchPublish(nil, arr) // batch send
		streaming.CheckErr(err)
	}
	elapsed := time.Since(start)
	fmt.Printf("%d messages, published in: %s\n", numberOfMessages*batchSize, elapsed)

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')

	err = consumer.UnSubscribe()
	streaming.CheckErr(err)
	err = client.DeleteStream(streamName) // Remove the streaming queue and the data
	streaming.CheckErr(err)
	err = client.Close()
	streaming.CheckErr(err)
	fmt.Println("Bye bye")
}
