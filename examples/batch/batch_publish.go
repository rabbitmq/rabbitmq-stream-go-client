package main

import (
	"bufio"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	"github.com/gsantomaggio/go-stream-client/pkg/stream"
	"os"
	"strconv"
	"sync"
	"time"
)

func main() {
	fmt.Println("RabbitMQ golang streaming client")
	fmt.Println("Connecting ...")
	var client = stream.NewStreamingClient()                                           // create Client Struct
	//address := "rabbitmq-stream://test:test@stream.4messages.net:5551/%2f"
	address := "rabbitmq-stream://guest:guest@localhost:5551/%2f"
	err := client.Connect(address) // Connect
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}
	fmt.Println("Connected!")
	streamName := uuid.New().String()
	fmt.Printf("Stream %s \n", streamName)
	_, err = client.CreateStream(streamName) // Create the streaming queue
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}
	producers := 10
	consumers := 10
	iterations := 4000
	messages := 100
	counters := map[byte]int64{}
	var lock = sync.RWMutex{}

	go func() {
		for {
			time.Sleep(5 * time.Second)
			lock.RLock()

			fmt.Printf("Counters: %d \n", counters)
			lock.RUnlock()
		}

	}()
	for i := 0; i < consumers; i++ {
		_, err := client.NewConsumer(streamName, func(subscriberId byte, message *amqp.Message) {
			lock.Lock()
			defer lock.Unlock()
			counters[subscriberId] = counters[subscriberId] + 1
			//fmt.Printf("data: %s \n", message.Data)
		})
		if err != nil {
			fmt.Printf("error: %s", err)
			return
		}
	}

	fmt.Printf("starting with %d producers, %d messages and  %d iterations \n", producers, iterations, messages)

	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		producer, err := client.NewProducer(streamName) // Get a new subscribe to publish the messages
		if err != nil {
			fmt.Printf("error: %s", err)
			return
		}
		go func(id int, producer *stream.Producer, wg *sync.WaitGroup) {
			defer wg.Done()
			for z := 0; z < iterations; z++ {

				var arr []*amqp.Message // amqp 1.0 message from https://github.com/Azure/go-amqp
				for i := 0; i < messages; i++ {
					arr = append(arr, amqp.NewMessage([]byte("hello world_"+strconv.Itoa(z) + "-" +strconv.Itoa(i) )))
				}
				_, err = producer.BatchPublish(nil, arr) // batch send
				if err != nil {
					fmt.Printf("error: %s", err)
					return
				}
			}
		}(i, producer, &wg)
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%s  to publish %d messages\n", elapsed, producers*iterations*messages)

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press any key to un-subscribe ")
	_, _ = reader.ReadString('\n')
	err = client.UnSubscribeAll()
	if err != nil {
		fmt.Printf("UnSubscribeAll error: %s", err)
		return
	}
	fmt.Println("Press any key to finish ")
	_, _ = reader.ReadString('\n')

	fmt.Println("Closing all producers ")
	err = client.CloseAllProducers()
	if err != nil {
		fmt.Printf("CloseAllProducers: %s", err)
		return
	}
	_, err = client.DeleteStream(streamName) // Remove the streaming queue and the data
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}

	fmt.Println("Bye bye")
}
