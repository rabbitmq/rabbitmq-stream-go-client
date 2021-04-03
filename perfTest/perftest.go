package main

import (
	"github.com/gsantomaggio/go-stream-client/perfTest/cmd"
	"time"
)

func main() {
	cmd.Execute()
	for true {
		time.Sleep(1 * time.Second)
	}
}
