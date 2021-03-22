package main

import (
	"bufio"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/gsantomaggio/go-stream-client/pkg/stream"
	"os"
	"time"
)



func main() {
	fmt.Println("Getting started with Streaming client for RabbitMQ")
	fmt.Println("Connecting to RabbitMQ streaming ...")
	var client = stream.NewStreamingClient()                                  // create Client Struct
	err := client.Connect("rabbitmq-stream://guest:guest@localhost:5551/%2f") // Connect
	if err != nil {
		fmt.Printf("Error during connection: %s", err)
		return
	}
	fmt.Println("Connected to localhost")
	streamName := "golang-stream"
	_, err = client.CreateStream(streamName) // Create the streaming queue
	if err != nil {
		fmt.Printf("Error creating stream: %s", err)
		return
	}



	// Get a new subscribe to publish the messages
	producer, err := client.NewProducer(streamName)
	if err != nil {
		fmt.Printf("Error creating subscribe: %s", err)
		return
	}
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
		if err != nil {
			fmt.Printf("Error publish: %s", err)
			return
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("%d messages, published in: %s\n", numberOfMessages*batchSize, elapsed)



	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')

	_, err = client.DeleteStream(streamName) // Remove the streaming queue and the data
	if err != nil {
		fmt.Printf("error deleting stream: %s \n", err)
		return
	}
	err = client.Close()
	if err != nil {
		fmt.Printf("error closing client: %s \n", err)
		return
	}
	fmt.Println("Bye bye")
}
