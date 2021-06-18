package stream

import (
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"strconv"
)

func CreateArrayMessagesForTesting(numberOfMessages int) []message.StreamMessage {
	var arr []message.StreamMessage
	for z := 0; z < numberOfMessages; z++ {
		arr = append(arr, amqp.NewMessage([]byte("test_"+strconv.Itoa(z))))
	}
	return arr

}
