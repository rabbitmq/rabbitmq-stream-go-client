package main

import (
	"bufio"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/gsantomaggio/go-stream-client/pkg/stream"
	"os"
	"strconv"
	"sync"
	"time"
)

func main() {
	fmt.Println("Connecting ...")
	var client = stream.NewStreamingClient()                                  // create Client Struct
	err := client.Connect("rabbitmq-stream://guest:guest@localhost:5551/%2f") // Connect
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}
	fmt.Println("Connected!")
	streamName := "golang-stream"
	_, err = client.CreateStream(streamName) // Create the streaming queue
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}

	var arr []*amqp.Message // amqp 1.0 message from https://github.com/Azure/go-amqp
	for z := 0; z < 100; z++ {
		arr = append(arr, amqp.NewMessage([]byte("hello world_"+strconv.Itoa(z))))
	}

	{
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			producer, err := client.NewProducer(streamName) // Get a new producer to publish the messages
			if err != nil {
				fmt.Printf("error: %s", err)
				return
			}
			go func(id int, producer *stream.Producer, wg *sync.WaitGroup) {
				defer wg.Done()
				fmt.Printf("starting producer: %d, item: %d \n", producer.ProducerID, id)
				start := time.Now()
				for z := 0; z < 1000; z++ {
					_, err = producer.BatchPublish(nil, arr) // batch send
					if err != nil {
						fmt.Printf("error: %s", err)
						return
					}
				}
				elapsed := time.Since(start)
				fmt.Printf("end producer: %d, item: %d took %s\n", producer.ProducerID, id, elapsed)

			}(i, producer, &wg)
		}
		wg.Wait()
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Press any key to finish ")
	_, _ = reader.ReadString('\n')

	fmt.Print("Closing all producers ")
	err = client.CloseAllProducers()
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}
	_, err = client.DeleteStream(streamName) // Remove the streaming queue and the data
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}
	fmt.Println("Bye bye")
}
