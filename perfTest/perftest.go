package main

import (
	"github.com/rabbitmq/rabbitmq-stream-go-client/perfTest/cmd"
	"sync"
)

var wg sync.WaitGroup

func main() {

	wg.Add(1)
	go cmd.Execute()
	wg.Wait()
}
