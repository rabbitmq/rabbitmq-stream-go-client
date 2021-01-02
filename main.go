package main

import (
	"fmt"
	"github.com/Azure/go-amqp"
	stream "github.com/gsantomaggio/go-stream-client/internals/stream"
	"strconv"
	"time"
)

func main() {
	var client = stream.Client{}
	_ = client.Create()
	streamName := "go-java-stream-7"
	client.CreateStream(streamName)
	client.DeclarePublisher(0, streamName)
	t1 := time.Now()
	for i := 0; i < 100; i++ {
		var arr []*amqp.Message
		for z := 0; z < 100; z++ {
			arr = append(arr, amqp.NewMessage([]byte("hello world_"+strconv.Itoa(i))))
		}
		client.BatchPublish(arr)
		//time.Sleep(3 * time.Millisecond)
	}
	t2 := time.Now()
	diff := t2.Sub(t1)
	fmt.Printf("end: %f", diff.Seconds())
	time.Sleep(5 * time.Second)
}
