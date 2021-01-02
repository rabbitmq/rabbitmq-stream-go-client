# GO stream client
---
A POC client for [RabbitMQ Stream Queues](https://github.com/rabbitmq/rabbitmq-server/tree/master/deps/rabbitmq_stream)

### How to use
---

```golang
	var client = stream.Client{}
	err := client.Create()
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}
	streamName := "go-java-stream-7"
	err = client.CreateStream(streamName)
	if err != nil {
		fmt.Printf("error: %s", err)
		return
	}

	client.DeclarePublisher(0, streamName)
	for i := 0; i < 100; i++ {
	    var arr []*amqp.Message
		for z := 0; z < 100; z++ {
			arr = append(arr, amqp.NewMessage([]byte("hello world_"+strconv.Itoa(i))))
		}
		client.BatchPublish(arr)
	}

```
 