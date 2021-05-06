package streaming

import (
	"bytes"
	"context"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
)

type PublishConfirm func(ch <-chan []int64)

type Producer struct {
	ID             uint8
	options        *ProducerOptions
	onClose        onClose
	publishConfirm PublishConfirm
}

type ProducerOptions struct {
	client         *Client
	streamName     string
	publishConfirm PublishConfirm
}

func NewProducerOptions() *ProducerOptions {
	return &ProducerOptions{}
}

func (c *ProducerOptions) Stream(streamName string) *ProducerOptions {
	c.streamName = streamName
	return c
}

func (c *ProducerOptions) OnPublishConfirm(publishConfirm PublishConfirm) *ProducerOptions {
	c.publishConfirm = publishConfirm
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
	writeProtocolHeader(b, length, commandPublish)
	writeByte(b, publishId)
	writeInt(b, len(messages)) //toExcluded - fromInclude

	var seq int64
	seq = 0
	for _, msg := range messages {
		r, _ := msg.MarshalBinary()
		writeLong(b, seq)   // sequence
		writeInt(b, len(r)) // len
		b.Write(r)
		seq += 1
	}

	err := producer.options.client.socket.writeAndFlush(b.Bytes())
	if err != nil {
		return 0, err
	}
	return 0, nil
}

func (producer *Producer) Close() error {
	if !producer.options.client.socket.isOpen() {
		return fmt.Errorf("connection already closed")
	}

	err := producer.options.client.deletePublisher(producer.ID)
	if err != nil {
		return err
	}
	if producer.options.client.coordinator.ProducersCount() == 0 {
		err := producer.options.client.Close()
		if err != nil {
			return err
		}
	}
	ch := make(chan uint8, 1)
	ch <- producer.ID
	producer.onClose(ch)
	close(ch)

	return nil
}

func (c *Client) deletePublisher(publisherId byte) error {
	length := 2 + 2 + 4 + 1
	resp := c.coordinator.NewResponse()
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandDeletePublisher,
		correlationId)

	writeByte(b, publisherId)
	errWrite := c.handleWrite(b.Bytes(), resp)

	err := c.coordinator.RemoveProducerById(publisherId)
	if err != nil {
		//TODO LOGWARN
		WARN("Error RemoveProducerById %d", publisherId)
	}

	return errWrite
}
