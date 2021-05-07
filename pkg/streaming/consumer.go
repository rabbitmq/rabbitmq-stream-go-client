package streaming

import (
	"bytes"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"sync"
)

type Consumer struct {
	ID              uint8
	response        *Response
	offset          int64
	options         *ConsumerOptions
	onClose         onClose
	mutex           *sync.RWMutex
	messagesHandler MessagesHandler
}

func (consumer *Consumer) GetStream() string {
	return consumer.options.streamName
}

func (consumer *Consumer) setOffset(offset int64) {
	consumer.mutex.Lock()
	defer consumer.mutex.Unlock()
	consumer.offset = offset
}

func (consumer *Consumer) getOffset() int64 {
	consumer.mutex.RLock()
	defer consumer.mutex.RUnlock()
	return consumer.offset
}

type ConsumerContext struct {
	Consumer *Consumer
}

type MessagesHandler func(Context ConsumerContext, message *amqp.Message)

type ConsumerOptions struct {
	client              *Client
	consumerName        string
	streamName          string
	autocommit          bool
	offsetSpecification OffsetSpecification
}

func NewConsumerOptions() *ConsumerOptions {
	return &ConsumerOptions{
		offsetSpecification: OffsetSpecification{}.Last(),
		autocommit:          true}
}

func (c *ConsumerOptions) Name(consumerName string) *ConsumerOptions {
	c.consumerName = consumerName
	return c
}

func (c *ConsumerOptions) Stream(streamName string) *ConsumerOptions {
	c.streamName = streamName
	return c
}

//func (c *ConsumerOptions) AutoCommit() *ConsumerOptions {
//	c.autocommit = true
//	return c
//}
func (c *ConsumerOptions) ManualCommit() *ConsumerOptions {
	c.autocommit = false
	return c
}
func (c *ConsumerOptions) Offset(offsetSpecification OffsetSpecification) *ConsumerOptions {
	c.offsetSpecification = offsetSpecification
	return c
}

func (c *Client) credit(subscriptionId byte, credit int16) {
	length := 2 + 2 + 1 + 2
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandCredit)
	writeByte(b, subscriptionId)
	writeShort(b, credit)
	err := c.socket.writeAndFlush(b.Bytes())
	if err != nil {
		WARN("credit error:%s", err)
	}
}

func (consumer *Consumer) UnSubscribe() error {
	length := 2 + 2 + 4 + 1
	resp := consumer.options.client.coordinator.NewResponse(commandUnsubscribe)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandUnsubscribe,
		correlationId)

	writeByte(b, consumer.ID)
	err := consumer.options.client.handleWrite(b.Bytes(), resp)
	consumer.response.code <- Code{id: closeChannel}
	errC := consumer.options.client.coordinator.RemoveConsumerById(consumer.ID)
	if errC != nil {
		WARN("Error during remove consumer id:%s", errC)
	}

	if consumer.options.client.coordinator.ConsumersCount() == 0 {
		err := consumer.options.client.Close()
		if err != nil {
			return err
		}
	}

	ch := make(chan uint8, 1)
	ch <- consumer.ID
	consumer.onClose(ch)
	close(ch)
	return err
}

func (consumer *Consumer) Commit() error {
	if consumer.options.streamName == "" {
		return fmt.Errorf("stream name can't be empty")
	}
	length := 2 + 2 + 4 + 2 + len(consumer.options.consumerName) + 2 +
		len(consumer.options.streamName) + 8
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandCommitOffset,
		0) // correlation ID not used yet, may be used if commit offset has a confirm

	writeString(b, consumer.options.consumerName)
	writeString(b, consumer.options.streamName)

	writeLong(b, consumer.getOffset())
	return consumer.options.client.socket.writeAndFlush(b.Bytes())

}

func (consumer *Consumer) QueryOffset() (int64, error) {
	length := 2 + 2 + 4 + 2 + len(consumer.options.consumerName) + 2 + len(consumer.options.streamName)

	resp := consumer.options.client.coordinator.NewResponse(commandQueryOffset)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandQueryOffset,
		correlationId)

	writeString(b, consumer.options.consumerName)
	writeString(b, consumer.options.streamName)
	err := consumer.options.client.handleWriteWithResponse(b.Bytes(), resp, false)
	if err != nil {
		return 0, err

	}

	offset := <-resp.data
	_ = consumer.options.client.coordinator.RemoveResponseById(resp.correlationid)

	return offset.(int64), nil

}

/*
Offset constants
*/
const (
	typeFirst        = int16(1)
	typeLast         = int16(2)
	typeNext         = int16(3)
	typeOffset       = int16(4)
	typeTimestamp    = int16(5)
	typeLastConsumed = int16(6)
)

type OffsetSpecification struct {
	typeOfs int16
	offset  int64
}

func (o OffsetSpecification) First() OffsetSpecification {
	o.typeOfs = typeFirst
	return o
}

func (o OffsetSpecification) Last() OffsetSpecification {
	o.typeOfs = typeLast
	return o
}

func (o OffsetSpecification) Next() OffsetSpecification {
	o.typeOfs = typeNext
	return o
}

func (o OffsetSpecification) Offset(offset int64) OffsetSpecification {
	o.typeOfs = typeOffset
	o.offset = offset
	return o
}

func (o OffsetSpecification) Timestamp(offset int64) OffsetSpecification {
	o.typeOfs = typeTimestamp
	o.offset = offset
	return o
}

func (o OffsetSpecification) isOffset() bool {
	return o.typeOfs == typeOffset || o.typeOfs == typeLastConsumed
}

func (o OffsetSpecification) isLastConsumed() bool {
	return o.typeOfs == typeLastConsumed
}
func (o OffsetSpecification) isTimestamp() bool {
	return o.typeOfs == typeTimestamp
}

func (o OffsetSpecification) LastConsumed() OffsetSpecification {
	o.typeOfs = typeLastConsumed
	o.offset = -1
	return o
}
