package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/gsantomaggio/go-stream-client/pkg/stream"
	"os"
	"strconv"
	"time"
)

func main() {
	fmt.Println("Connecting ...")
	ctx := context.Background()
	var client = stream.NewStreamingClient()                                  // create Client Struct
	err := client.Connect("rabbitmq-stream://guest:guest@localhost:5551/%2f") // Connect
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}
	fmt.Println("Connected!")
	streamName := "my-streaming-queue-5"
	err = client.CreateStream(streamName) // Create the streaming queue
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}

	var arr []*amqp.Message // amqp 1.0 message from https://github.com/Azure/go-amqp
	for z := 0; z < 100; z++ {
		arr = append(arr, amqp.NewMessage([]byte("hello world_"+strconv.Itoa(z))))
	}

	{
		for i := 0; i < 20; i++ {
			producer, err := client.NewProducer(streamName) // Get a new producer to publish the messages
			if err != nil {
				fmt.Printf("error: %s", err)
				return
			}
			go func(id int, producer *stream.Producer) {
				fmt.Printf("starting producer: %d, item: %d \n", producer.ProducerID, id)
				start := time.Now()
				for z := 0; z < 1000; z++ {
					_, err = producer.BatchPublish(ctx, arr) // batch send
					if err != nil {
						fmt.Printf("error: %s", err)
						return
					}
				}
				elapsed := time.Since(start)
				fmt.Printf("end producer: %d, item: %d took %s\n", producer.ProducerID, id, elapsed)

			}(i, producer)
		}
	}
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Press any key to finish ")
	_, _ = reader.ReadString('\n')
	err = stream.GetProducers().CloseAllProducers()
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}

	err = client.DeleteStream(streamName) // Remove the streaming queue and the data
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}
	fmt.Print("Bye bye")
}
