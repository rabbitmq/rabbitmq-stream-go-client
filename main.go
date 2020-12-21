package main

import (
	"fmt"
	stream2 "github.com/gsantomaggio/go-stream-client/internals/stream"
	"time"
)

func main() {

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

}
