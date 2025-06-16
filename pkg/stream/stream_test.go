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
	arr := make([]message.StreamMessage, numberOfMessages)
	for i := range numberOfMessages {
		arr[i] = CreateMessageForTesting(prefix, i)
	}
	return arr
}
func CreateMessageForTesting(prefix string, index int) message.StreamMessage {
	return amqp.NewMessage([]byte(prefix + strconv.Itoa(index)))
}
