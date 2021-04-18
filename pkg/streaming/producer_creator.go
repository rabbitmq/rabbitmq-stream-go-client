package streaming

import (
	"bytes"
	"context"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
)

type Producer struct {
	ID uint8
	//response   *Response
	parameters *ProducerCreator
}

type ProducerCreator struct {
	client     *Client
	streamName string
}

func (c *Client) ProducerCreator() *ProducerCreator {
	return &ProducerCreator{client: c}
}

func (c *ProducerCreator) Stream(streamName string) *ProducerCreator {
	c.streamName = streamName
	return c
}

func (c *ProducerCreator) Build() (*Producer, error) {
	producer, err := c.client.coordinator.NewProducer(c)
	if err != nil {
		return nil, err
	}
	publisherReferenceSize := 0
	length := 2 + 2 + 4 + 1 + 2 + publisherReferenceSize + 2 + len(c.streamName)
	resp := c.client.coordinator.NewResponse()
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandDeclarePublisher)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteByte(b, producer.ID)
	WriteShort(b, int16(publisherReferenceSize))
	WriteString(b, c.streamName)
	res := c.client.HandleWrite(b.Bytes(), resp)
	return producer, res

}

func (producer *Producer) BatchPublish(ctx context.Context, msgs []*amqp.Message) (int, error) {
	frameHeaderLength := 2 + 2 + 1 + 4
	var msgLen int
	for _, msg := range msgs {
		r, _ := msg.MarshalBinary()
		msgLen += len(r) + 8 + 4
	}

	length := frameHeaderLength + msgLen
	publishId := producer.ID
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandPublish)
	WriteUShort(b, Version1)
	WriteByte(b, publishId)
	WriteInt(b, len(msgs)) //toExcluded - fromInclude

	var seq int64
	seq = 0
	for _, msg := range msgs {
		r, _ := msg.MarshalBinary()
		WriteLong(b, seq)   // sequence
		WriteInt(b, len(r)) // len
		b.Write(r)
		seq += 1
	}

	err := producer.parameters.client.socket.writeAndFlush(b.Bytes())
	if err != nil {
		return 0, err
	}
	return 0, nil
}

func (producer *Producer) Close() error {
	return producer.parameters.client.deletePublisher(producer.ID)
}

func (c *Client) deletePublisher(publisherId byte) error {
	length := 2 + 2 + 4 + 1
	resp := c.coordinator.NewResponse()
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandDeletePublisher)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteByte(b, publisherId)
	errWrite := c.HandleWrite(b.Bytes(), resp)

	err := c.coordinator.RemoveProducerById(publisherId)
	if err != nil {
		//TODO LOGWARN
		WARN("Error RemoveProducerById %d", publisherId)
	}

	return errWrite
}
