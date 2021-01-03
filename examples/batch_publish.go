package main

import (
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/gsantomaggio/go-stream-client/internals/stream"
	"strconv"
	"time"
)

func main() {
	fmt.Println("Connecting ...")
	var client = stream.Client{}
	err := client.Connect("rabbitmq-stream://guest:guest@localhost:5555/%2f") // Connect
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}
	fmt.Println("Connected!")
	streamName := "go-java-stream-7"
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
	for i := 0; i < 100; i++ {
		var arr []*amqp.Message
		for z := 0; z < 100; z++ {
			arr = append(arr, amqp.NewMessage([]byte("hello world_"+strconv.Itoa(i))))
		}
		err = producer.BatchPublish(arr)
		if err != nil {
			fmt.Printf("error: %s", err)
			return
		}

	}
	t2 := time.Now()
	diff := t2.Sub(t1)
	fmt.Printf("end: %f", diff.Seconds())
	time.Sleep(5 * time.Second)
}
