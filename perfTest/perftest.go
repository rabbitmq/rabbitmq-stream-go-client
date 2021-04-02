package main

import (
	"fmt"
	"github.com/gsantomaggio/go-stream-client/perfTest/cmd"
	"time"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("Error operation: %s \n", err)
	}
}
func main() {
	cmd.Execute()
	for true {
		time.Sleep(1 * time.Second)
	}
}
