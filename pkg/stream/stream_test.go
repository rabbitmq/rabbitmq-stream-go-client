package stream

import (
	"strconv"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
)

const (
	MessageBufferTooBig             = 1148001
	MessageBufferBigButLessTheFrame = 1048400
	LoweredFrameSizeBytes           = 512 * 1024
	MessageBufferAboveLoweredFrame  = 768 * 1024 // > LoweredFrameSizeBytes but < broker DEFAULT_FRAME_MAX
	MessageBufferBelowLoweredFrame  = 64 * 1024
)

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
