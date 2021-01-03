# GO stream client
---
A POC client for [RabbitMQ Stream Queues](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)

### How to use
---

```golang
    package main
    
    import (
    	"bufio"
    	"context"
    	"fmt"
    	"github.com/Azure/go-amqp"
    	"github.com/gsantomaggio/go-stream-client/pkg/stream"
    	"os"
    	"strconv"
    	"time"
    )
    
    func main() {
    	fmt.Println("Connecting ...")
    	ctx := context.Background()
    	var client = stream.NewStreamingClient()                                  // create Client Struct
    	err := client.Connect("rabbitmq-stream://guest:guest@localhost:5555/%2f") // Connect
    	if err != nil {
    		fmt.Printf("error: %s", err)
    		return
    	}
    	fmt.Println("Connected!")
    	streamName := "my-streaming-queue-5"
    	err = client.CreateStream(streamName) // Create the streaming queue
    	if err != nil {
    		fmt.Printf("error: %s", err)
    		return
    	}
    
    	producer, err := client.NewProducer(streamName) // Get a new producer to publish the messages
    	if err != nil {
    		fmt.Printf("error: %s", err)
    		return
    	}
    
    	var arr []*amqp.Message // amqp 1.0 message from https://github.com/Azure/go-amqp
    	for z := 0; z < 100; z++ {
    		arr = append(arr, amqp.NewMessage([]byte("hello world_"+strconv.Itoa(z))))
    	}
    
    	{
    		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
    		defer cancel()
    		_, err = producer.BatchPublish(ctx, arr) // batch send
    		if err != nil {
    			fmt.Printf("error: %s", err)
    			return
    		}
    
    	}
    	reader := bufio.NewReader(os.Stdin)
    	fmt.Print("Press any key to finish ")
    	_, _ = reader.ReadString('\n')
    	fmt.Print("Bye bye")
    }
```
 