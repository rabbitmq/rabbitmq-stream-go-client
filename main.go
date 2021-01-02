package main

import (
	"fmt"
<<<<<<< HEAD
	stream2 "github.com/gsantomaggio/go-stream-client/internals/stream"
=======
	"github.com/Azure/go-amqp"
	stream "github.com/gsantomaggio/go-stream-client/internals/stream"
	"strconv"
>>>>>>> Add publish batch
	"time"
)

func main() {
<<<<<<< HEAD

	var client = stream2.Client{}
	_ = client.Create()
	//fmt.Print("%s", []byte{'A', 'M', 'Q', 'P', 0x0, 1, 0, 0, 'Z'})
	streamName := "test-go-lang"
	//client.CreateStream(streamName)
	client.DeclarePublisher(0, streamName)
	dt := time.Now()
	fmt.Println("start: %s", dt.String())
	for i := 0; i < 10000; i++ {
		client.Publish("A")
		time.Sleep(3 * time.Second)
		fmt.Println("start: %s", dt.String())
	}
	dt = time.Now()
	fmt.Println("end: %s", dt.String())
=======
	var client = stream.Client{}
	_ = client.Create()
	streamName := "go-java-stream-6"
	client.CreateStream(streamName)
	client.DeclarePublisher(0, streamName)
	t1 := time.Now()
	for i := 0; i < 1000000; i++ {
		var arr []*amqp.Message
		for z := 0; z < 100; z++ {
			arr = append(arr, amqp.NewMessage([]byte("hello world_" + strconv.Itoa(i))))
		}
		client.BatchPublish(arr)
		//time.Sleep(3 * time.Millisecond)
	}
	t2 := time.Now()
	diff := t2.Sub(t1)
	fmt.Printf("end: %f", diff.Seconds())
	time.Sleep(5 * time.Second)
>>>>>>> Add publish batch

}
