package main

import (
	stream2 "github.com/gsantomaggio/go-stream-client/internals/stream"
)

func main() {

	var client = stream2.Client{}
	_ = client.Create()
//	client.CreateStream("my-golang-stream")
	client.DeclarePublisher(0,"my-golang-stream")
	client.Publish("hello")



}
