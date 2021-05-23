package stream

import (
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"strconv"
)

func CreateArrayMessagesForTesting(numberOfMessages int) []*amqp.Message {
	var arr []*amqp.Message
	for z := 0; z < numberOfMessages; z++ {
		arr = append(arr, amqp.NewMessage([]byte("test_"+strconv.Itoa(z))))
	}
	return arr

}