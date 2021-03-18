package main

import (
	"bufio"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	"github.com/gsantomaggio/go-stream-client/pkg/stream"
	"os"
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
	streamName := uuid.New().String()
	_, err = client.CreateStream(streamName) // Create the streaming queue
	if err != nil {
		fmt.Printf("Error creating stream: %s", err)
		return
	}

	count := 0
	consumer, err := client.NewConsumer(streamName, func(subscriberId byte, message *amqp.Message) {
		count++
		if count%10000 == 0 {
			fmt.Printf("id: %d - body: %s - Total mssages consumed:%d \n", subscriberId, message.Data, count)
		}
	})
	if err != nil {
		fmt.Printf("Error NewConsumer: %s", err)
		return
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	consumer.UnSubscribe()
	fmt.Println("Bye bye!")
}
