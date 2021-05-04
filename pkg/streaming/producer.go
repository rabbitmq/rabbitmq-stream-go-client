package streaming

import (
	"bytes"
	"context"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
)

type onConsumerClosed func(ch <-chan uint8)

type Producer struct {
	ID uint8
	//response   *Response
	parameters       *ProducerOptions
	onConsumerClosed onConsumerClosed
}

type ProducerOptions struct {
	client     *Client
	streamName string
}

func (c *ProducerOptions) Stream(streamName string) *ProducerOptions {
	c.streamName = streamName
	return c
}

func (producer *Producer) BatchPublish(ctx context.Context, messages []*amqp.Message) (int, error) {
	frameHeaderLength := 2 + 2 + 1 + 4
	var msgLen int
	for _, msg := range messages {
		r, _ := msg.MarshalBinary()
		msgLen += len(r) + 8 + 4
	}

	length := frameHeaderLength + msgLen
	publishId := producer.ID
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteProtocolHeader(b, length, CommandPublish)
	WriteByte(b, publishId)
	WriteInt(b, len(messages)) //toExcluded - fromInclude

	var seq int64
	seq = 0
	for _, msg := range messages {
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
	if !producer.parameters.client.socket.isOpen() {
		return fmt.Errorf("connection already closed")
	}

	err := producer.parameters.client.deletePublisher(producer.ID)
	if err != nil {
		return err
	}
	if producer.parameters.client.coordinator.ProducersCount() == 0 {
		err := producer.parameters.client.Close()
		if err != nil {
			return err
		}
	}
	ch := make(chan uint8)
	//ch <- producer.ID
	producer.onConsumerClosed(ch)

	return nil
}

func (c *Client) deletePublisher(publisherId byte) error {
	length := 2 + 2 + 4 + 1
	resp := c.coordinator.NewResponse()
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteProtocolHeader(b, length, CommandDeletePublisher,
		correlationId)

	WriteByte(b, publisherId)
	errWrite := c.HandleWrite(b.Bytes(), resp)

	err := c.coordinator.RemoveProducerById(publisherId)
	if err != nil {
		//TODO LOGWARN
		WARN("Error RemoveProducerById %d", publisherId)
	}

	return errWrite
}
