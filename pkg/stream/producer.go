package stream

import (
	"bytes"
	"context"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
)

type Producer struct {
	ID             uint8
	options        *ProducerOptions
	onClose        onInternalClose
	publishConfirm PublishConfirmListener
}

type ProducerOptions struct {
	client     *Client
	streamName string
	Name       string
}

func (po *ProducerOptions) SetProducerName(name string) *ProducerOptions {
	po.Name = name
	return po
}

func NewProducerOptions() *ProducerOptions {
	return &ProducerOptions{}
}

func (producer *Producer) BatchPublish(ctx context.Context, batchMessages []*amqp.Message) (int, error) {
	if len(batchMessages) > 1000 {
		return 0, fmt.Errorf("%d - %s", len(batchMessages), "too many messages")
	}

	frameHeaderLength := 2 + 2 + 1 + 4
	var msgLen int
	for _, msg := range batchMessages {
		r, _ := msg.MarshalBinary()
		msgLen += len(r) + 8 + 4
	}

	length := frameHeaderLength + msgLen
	publishId := producer.ID
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandPublish)
	writeByte(b, publishId)
	writeInt(b, len(batchMessages)) //toExcluded - fromInclude

	var seq int64
	seq = 0
	for _, msg := range batchMessages {
		r, _ := msg.MarshalBinary()
		writeLong(b, seq)   // sequence
		writeInt(b, len(r)) // len
		b.Write(r)
		seq += 1
	}

	bufferToWrite := b.Bytes()
	if len(bufferToWrite) > producer.options.client.tuneState.requestedMaxFrameSize {
		return 0, fmt.Errorf("%s", lookErrorCode(responseCodeFrameTooLarge))
	}

	err := producer.options.client.socket.writeAndFlush(b.Bytes())
	if err != nil {
		return 0, err
	}
	return len(batchMessages), nil
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
	resp := c.coordinator.NewResponse(commandDeletePublisher)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandDeletePublisher,
		correlationId)

	writeByte(b, publisherId)
	errWrite := c.handleWrite(b.Bytes(), resp)

	err := c.coordinator.RemoveProducerById(publisherId)
	if err != nil {
		//TODO LOGWARN
		logWarn("Error RemoveProducerById %d", publisherId)
	}

	return errWrite
}
