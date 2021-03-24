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

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("Error operation: %s \n", err)
	}
}
func main() {
	fmt.Println("Getting started with Streaming client for RabbitMQ")
	fmt.Println("Connecting to RabbitMQ streaming ...")
	uris := "rabbitmq-streaming://guest:guest@localhost:5551/%2f"
	client, err := streaming.NewClientCreator().Uri(uris).Connect() // create Client Struct
	CheckErr(err)

	fmt.Println("Connected to localhost")
	streamName := "OffsetTest"
	err = client.StreamCreator().Stream(streamName).MaxAge(120 * time.Hour).Create() // Create the streaming queue

	var count int32

	CheckErr(err)
	consumer, err := client.ConsumerCreator().
		Stream(streamName).
		Name("my_consumer").
		//Offset(streaming.OffsetSpecification{}.Timestamp(time.Now().Unix())).
		MessagesHandler(func(consumerId uint8, message *amqp.Message) {
			atomic.AddInt32(&count, 1)
			fmt.Printf("Golang Counter:%d\n", count)
		}).Build()
	CheckErr(err)
	// Get a new producer to publish the messages
	producer, err := client.ProducerCreator().Stream(streamName).Build()
	CheckErr(err)
	numberOfMessages := 10
	batchSize := 5

	// Create AMQP 1.0 messages, see:https://github.com/Azure/go-amqp
	// message aggregation

	start := time.Now()
	for z := 0; z < numberOfMessages; z++ {
		var arr []*amqp.Message
		for f := 0; f < batchSize; f++ {
			arr = append(arr, amqp.NewMessage([]byte(fmt.Sprintf("test_%d_%d", z, f) )))
		}
		_, err = producer.BatchPublish(nil, arr) // batch send
		CheckErr(err)
	}
	elapsed := time.Since(start)
	fmt.Printf("%d messages, published in: %s\n", numberOfMessages*batchSize, elapsed)

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')

	err = consumer.UnSubscribe()
	CheckErr(err)
	err = client.DeleteStream(streamName) // Remove the streaming queue and the data
	CheckErr(err)
	err = client.Close()
	CheckErr(err)
	fmt.Println("Bye bye")
}
