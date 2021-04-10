package main

import (
	"github.com/gsantomaggio/go-stream-client/perfTest/cmd"
	"sync"
)

var wg sync.WaitGroup
func main() {

	wg.Add(1)
	go cmd.Execute()
	wg.Wait()
}
