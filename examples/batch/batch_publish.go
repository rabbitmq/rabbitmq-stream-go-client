package main

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/gsantomaggio/go-stream-client/pkg/stream"
	"strconv"
	"time"
)

func main() {
	fmt.Println("Connecting ...")
	ctx := context.Background()
	var client = stream.NewStreamingClient()
	err := client.Connect("rabbitmq-stream://guest:guest@localhost:5555/%2f") // Connect
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}
	fmt.Println("Connected!")
	streamName := "ml"
	err = client.CreateStream(streamName)
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}

	producer, err := client.NewProducer(streamName)

	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}
	t1 := time.Now()
	for i := 0; i < 100000; i++ {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		var arr []*amqp.Message
		for z := 0; z < 100; z++ {
			arr = append(arr, amqp.NewMessage([]byte("hello world_"+strconv.Itoa(i))))
		}

		_, err = producer.BatchPublish(ctx, arr)
		cancel()
		if err != nil {
			fmt.Printf("error: %s", err)
			return
		}

	}
	t2 := time.Now()
	diff := t2.Sub(t1)
	fmt.Printf("sent in: %f", diff.Seconds())
	time.Sleep(5 * time.Second)
}
