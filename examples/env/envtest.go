package main

import (
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/streaming"
	"strconv"
)

func CreateArrayMessagesForTesting(numberOfMessages int) []*amqp.Message {
	var arr []*amqp.Message
	for z := 0; z < numberOfMessages; z++ {
		arr = append(arr, amqp.NewMessage([]byte("test_"+strconv.Itoa(z))))
	}
	return arr
}
func main() {
	env, err := streaming.NewEnvironment(
		streaming.NewEnvironmentOptions().Host("34.76.8.103").UserName("replicator").
			Password("guest"),
	)
	if err != nil {
		return
	}

	streamname := uuid.New().String()
	err = env.DeclareStream(streamname, nil)

	producer, err := env.NewProducer(streamname, nil)
	if err != nil {
		return
	}

	_, err = producer.BatchPublish(nil, CreateArrayMessagesForTesting(100))
	if err != nil {
		return
	}
	err = producer.Close()
	if err != nil {
		return
	}

}
