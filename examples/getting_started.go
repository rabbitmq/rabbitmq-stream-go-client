package main

import (
	"bufio"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/gsantomaggio/go-stream-client/pkg/streaming"
	"os"
	"time"
)

func checkErr(err error) {
	if err != nil {
		fmt.Printf("Error operation: %s", err)
	}
}
func main() {
	fmt.Println("Getting started with Streaming client for RabbitMQ")
	fmt.Println("Connecting to RabbitMQ streaming ...")
	client, err := streaming.NewClientCreator().Connect() // create Client Struct
	checkErr(err)

	fmt.Println("Connected to localhost")
	streamName := "golang-streamingh12"
	err = client.StreamCreator().Stream(streamName).MaxAge(120 * time.Hour).Create() // Create the streaming queue

	checkErr(err)

	consumer, err := client.ConsumerCreator().
		Stream(streamName).
		Name("my_consumer").
		MessagesHandler(func(consumerId uint8, message *amqp.Message) {
			fmt.Printf("received %d, message %s \n", consumerId, message.Data)
		}).Build()
	checkErr(err)

	// Get a new producer to publish the messages
	producer, err := client.ProducerCreator().Stream(streamName).Build()
	checkErr(err)

	numberOfMessages := 100
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
		checkErr(err)

	}
	elapsed := time.Since(start)
	fmt.Printf("%d messages, published in: %s\n", numberOfMessages*batchSize, elapsed)

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')

	err = consumer.UnSubscribe()
	checkErr(err)

	err = client.DeleteStream(streamName) // Remove the streaming queue and the data
	checkErr(err)

	err = client.Close()
	checkErr(err)

	fmt.Println("Bye bye")
}
