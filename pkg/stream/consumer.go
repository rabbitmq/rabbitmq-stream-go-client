package stream

import (
	"bytes"
	"github.com/Azure/go-amqp"
)

type Handler interface {
	Messages(message *amqp.Message)
}

type Consumer struct {
	ID       uint8
	response *Response
	handler  Handler
}

func (client *Client) NewConsumer(stream string, handler Handler) (*Consumer, error) {

	return client.declareConsumer(stream, handler)
}

func (client *Client) declareConsumer(stream string, handler Handler) (*Consumer, error) {
	consumer := client.consumers.New(handler)
	length := 2 + 2 + 4 + 1 + 2 + len(stream) + 2 + 2 // misses the offset
	//if (offsetSpecification.isOffset() || offsetSpecification.isTimestamp()) {
	//	length += 8;
	//}
	resp := client.responses.New()
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))

	WriteInt(b, length)
	WriteUShort(b, CommandSubscribe)
	WriteUShort(b, Version1)
	WriteInt(b, correlationId)
	WriteByte(b, consumer.ID)

	WriteString(b, stream)

	WriteShort(b, 1)

	//	if (offsetSpecification.isOffset() || offsetSpecification.isTimestamp()) {
	//	WriteLong(offsetSpecification.getOffset());
	//}
	WriteShort(b, 10)

	client.writeAndFlush(b.Bytes())

	<-resp.code
	err := client.responses.RemoveById(correlationId)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}
