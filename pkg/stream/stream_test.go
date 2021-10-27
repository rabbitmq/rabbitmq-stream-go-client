package stream

import (
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"strconv"
)

func CreateArrayMessagesForTesting(numberOfMessages int) []message.StreamMessage {
	return CreateArrayMessagesForTestingWithPrefix("test_", numberOfMessages)

}
func CreateArrayMessagesForTestingWithPrefix(prefix string, numberOfMessages int) []message.StreamMessage {
	var arr []message.StreamMessage
	for z := 0; z < numberOfMessages; z++ {
		arr = append(arr, CreateMessageForTesting(prefix, z))
	}
	return arr
}
func CreateMessageForTesting(prefix string, index int) message.StreamMessage {
	return amqp.NewMessage([]byte(prefix + strconv.Itoa(index)))
}
