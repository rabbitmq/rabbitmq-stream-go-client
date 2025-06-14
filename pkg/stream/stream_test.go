package stream

import (
	"strconv"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
)

const MessageBufferTooBig = 1148001
const MessageBufferBigButLessTheFrame = 1048400

func CreateArrayMessagesForTesting(numberOfMessages int) []message.StreamMessage {
	return CreateArrayMessagesForTestingWithPrefix("test_", numberOfMessages)
}
func CreateArrayMessagesForTestingWithPrefix(prefix string, numberOfMessages int) []message.StreamMessage {
	var arr []message.StreamMessage
	for z := range numberOfMessages {
		arr = append(arr, CreateMessageForTesting(prefix, z))
	}
	return arr
}
func CreateMessageForTesting(prefix string, index int) message.StreamMessage {
	return amqp.NewMessage([]byte(prefix + strconv.Itoa(index)))
}
