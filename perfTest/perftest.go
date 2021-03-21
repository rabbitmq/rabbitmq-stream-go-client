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
	fmt.Println("PerfTest")
	fmt.Println("Connecting to RabbitMQ streaming ...")
	/// Constants
	numberOfMessages := 1_000_000
	batchSize := 100
	//numberOfProducers := 3
	///

	var client = stream.NewStreamingClient()                                           // create Client Struct
	err := client.Connect("rabbitmq-stream://test:test@stream.4messages.net:5551/%2f") // Connect
	if err != nil {
		fmt.Printf("Error during connection: %s", err)
		return
	}
	fmt.Println("Connected to localhost")
	streamName := "golang-stream-2"
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

	// Create AMQP 1.0 messages, see:https://github.com/Azure/go-amqp
	// message aggregation

	start := time.Now()
	for z := 0; z < numberOfMessages; z++ {
		var arr []*amqp.Message
		for f := 0; f < batchSize; f++ {
			arr = append(arr, amqp.NewMessage([]byte(fmt.Sprintf("test_%d_%d", z, f) )))
		}
		time.Sleep(20 * time.Millisecond)
		_, err = producer.BatchPublish(nil, arr) // batch send
		if err != nil {
			fmt.Printf("Error publish: %s", err)
			return
		}

		if z%1000 == 0 {
			elapsed := time.Since(start)
			fmt.Printf("%d messages, published in: %s\n", 1000*batchSize, elapsed)
			start = time.Now()

		}
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	fmt.Print("Closing all producers ")
	err = client.CloseAllProducers()
	if err != nil {
		fmt.Printf("error removing producers: %s", err)
		return
	}
	_, err = client.DeleteStream(streamName) // Remove the streaming queue and the data
	if err != nil {
		fmt.Printf("error deleting stream: %s \n", err)
		return
	}
	client.Close()
	fmt.Println("Bye bye")
}
