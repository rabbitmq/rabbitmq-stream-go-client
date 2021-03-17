package main

import (
	"bufio"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/gsantomaggio/go-stream-client/pkg/stream"
	"os"
)

type Sub struct {
	count int64
}

func (h *Sub) Messages(message *amqp.Message) {
	h.count++
	if h.count%1000 == 0 {
		fmt.Printf(" body: %s - Total mssages consumed: %d \n", message.Data, h.count)
	}

}

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

	sub := &Sub{}
	_, err = client.NewConsumer(streamName, sub)
	if err != nil {
		fmt.Printf("Error NewConsumer: %s", err)
		return
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')

	fmt.Println("Bye bye!")
}
