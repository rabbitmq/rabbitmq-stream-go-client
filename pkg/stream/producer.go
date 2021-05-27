package stream

import (
	"bytes"
	"context"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"sync"
	"sync/atomic"
)

type UnConfirmedMessage struct {
	Message    *amqp.Message
	ProducerID uint8
	MessageID  int64
}

type Producer struct {
	ID                  uint8
	options             *ProducerOptions
	onClose             onInternalClose
	unConfirmedMessages map[int64]*UnConfirmedMessage
	sequence            int64
	mutex               *sync.Mutex

	publishConfirm chan []*UnConfirmedMessage
	publishError   chan PublishError
	closeHandler   chan Event
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

func (producer *Producer) addUnConfirmed(messageid int64, message *amqp.Message, producerID uint8) {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	producer.unConfirmedMessages[messageid] = &UnConfirmedMessage{
		Message:    message,
		ProducerID: producerID,
		MessageID:  messageid,
	}
}

func (producer *Producer) removeUnConfirmed(messageid int64) {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	delete(producer.unConfirmedMessages, messageid)
}

func (producer *Producer) lenUnConfirmed() int {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return len(producer.unConfirmedMessages)
}

func (producer *Producer) getUnConfirmed(messageid int64) *UnConfirmedMessage {
	producer.mutex.Lock()
	defer producer.mutex.Unlock()
	return producer.unConfirmedMessages[messageid]
}

func (producer *Producer) NotifyPublishConfirmation() ChannelPublishConfirm {
	ch := make(chan []*UnConfirmedMessage, 1)
	producer.publishConfirm = ch
	return ch
}

func (producer *Producer) NotifyPublishError() ChannelPublishError {
	ch := make(chan PublishError, 1)
	producer.publishError = ch
	return ch
}

func (producer *Producer) NotifyClose() ChannelClose {
	ch := make(chan Event, 1)
	producer.closeHandler = ch
	return ch
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

	for _, msg := range batchMessages {
		id := atomic.AddInt64(&producer.sequence, 1)
		r, _ := msg.MarshalBinary()
		writeLong(b, id)    // sequence
		writeInt(b, len(r)) // len
		b.Write(r)
		producer.addUnConfirmed(id, msg, producer.ID)
	}

	bufferToWrite := b.Bytes()
	if len(bufferToWrite) > producer.options.client.tuneState.requestedMaxFrameSize {
		return 0, lookErrorCode(responseCodeFrameTooLarge)
	}

	err := producer.options.client.socket.writeAndFlush(b.Bytes())
	// TODO handle the socket read error to close the producer
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

func (producer *Producer) GetStreamName() string {
	if producer.options == nil {
		return ""
	}
	return producer.options.streamName
}

func (producer *Producer) GetName() string {
	if producer.options == nil {
		return ""
	}
	return producer.options.Name
}

func (c *Client) deletePublisher(publisherId byte) error {
	length := 2 + 2 + 4 + 1
	resp := c.coordinator.NewResponse(CommandDeletePublisher)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, CommandDeletePublisher,
		correlationId)

	writeByte(b, publisherId)
	errWrite := c.handleWrite(b.Bytes(), resp)

	//producer, _ := c.coordinator.GetProducerById(publisherId)
	// if there are UnConfirmed messages here, most likely there will be an
	// publisher error. Just try to wait a bit to receive the call back

	err := c.coordinator.RemoveProducerById(publisherId, Event{
		Command: CommandDeletePublisher,
		Reason:  "deletePublisher",
		Err:     nil,
	})
	if err != nil {
		logWarn("producer id: %d already removed", publisherId)
	}

	return errWrite.Err
}
