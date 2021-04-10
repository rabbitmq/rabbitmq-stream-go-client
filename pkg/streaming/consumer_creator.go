package streaming

import (
	"bytes"
	"fmt"
	"github.com/Azure/go-amqp"
	"sync"
)

type Consumer struct {
	ID         uint8
	response   *Response
	offset     int64
	parameters *ConsumerCreator
	mutex      *sync.RWMutex
}

func (consumer *Consumer) GetStream() string {
	return consumer.parameters.streamName
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

	//long offset();

	//void commit();

	Consumer *Consumer
}

type MessagesHandler func(Context ConsumerContext, message *amqp.Message)

type ConsumerCreator struct {
	client              *Client
	consumerName        string
	streamName          string
	messagesHandler     MessagesHandler
	autocommit          bool
	offsetSpecification OffsetSpecification
}

func (c *Client) ConsumerCreator() *ConsumerCreator {
	return &ConsumerCreator{client: c,
		offsetSpecification: OffsetSpecification{}.Last(),
		autocommit:          true}
}

func (c *ConsumerCreator) Name(consumerName string) *ConsumerCreator {
	c.consumerName = consumerName
	return c
}

func (c *ConsumerCreator) Stream(streamName string) *ConsumerCreator {
	c.streamName = streamName
	return c
}

func (c *ConsumerCreator) MessagesHandler(handlerFunc MessagesHandler) *ConsumerCreator {
	c.messagesHandler = handlerFunc
	return c
}

//func (c *ConsumerCreator) AutoCommit() *ConsumerCreator {
//	c.autocommit = true
//	return c
//}
func (c *ConsumerCreator) ManualCommit() *ConsumerCreator {
	c.autocommit = false
	return c
}
func (c *ConsumerCreator) Offset(offsetSpecification OffsetSpecification) *ConsumerCreator {
	c.offsetSpecification = offsetSpecification
	return c
}

func (c *ConsumerCreator) Build() (*Consumer, error) {
	consumer := c.client.coordinator.NewConsumer(c)
	length := 2 + 2 + 4 + 1 + 2 + len(c.streamName) + 2 + 2
	if c.offsetSpecification.isOffset() ||
		c.offsetSpecification.isTimestamp() {
		length += 8
	}

	if c.offsetSpecification.isLastConsumed() {
		lastOffset, err := consumer.QueryOffset()
		if err != nil {
			_ = c.client.coordinator.RemoveConsumerById(consumer.ID)
			return nil, err
		}
		c.offsetSpecification.offset = lastOffset
		// here we change the type since typeLastConsumed is not part of the protocol
		c.offsetSpecification.typeOfs = typeOffset
	}
	resp := c.client.coordinator.NewResponse()
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))

	WriteInt(b, length)
	WriteUShort(b, CommandSubscribe)
	WriteUShort(b, Version1)
	WriteInt(b, correlationId)
	WriteByte(b, consumer.ID)

	WriteString(b, c.streamName)

	WriteShort(b, c.offsetSpecification.typeOfs)

	if c.offsetSpecification.isOffset() ||
		c.offsetSpecification.isTimestamp() {
		WriteLong(b, c.offsetSpecification.offset)
	}
	WriteShort(b, 10)

	res := c.client.HandleWrite(b.Bytes(), resp)

	go func() {
		for true {
			select {
			case code := <-consumer.response.code:
				if code.id == CloseChannel {

					return
				}

			case data := <-consumer.response.data:
				consumer.setOffset(data.(int64))

			case messages := <-consumer.response.messages:
				for _, message := range messages {
					c.messagesHandler(ConsumerContext{Consumer: consumer}, message)
				}
			}
		}
	}()

	return consumer, res

}

func (c *Client) credit(subscriptionId byte, credit int16) {
	//if (credit < 0 || credit > Short.MAX_VALUE) {
	//throw new IllegalArgumentException("Credit value must be between 0 and " + Short.MAX_VALUE);
	//}
	length := 2 + 2 + 1 + 2
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandCredit)
	WriteShort(b, Version1)
	WriteByte(b, subscriptionId)
	WriteShort(b, credit)
	c.socket.writeAndFlush(b.Bytes())
}

func (consumer *Consumer) UnSubscribe() error {
	length := 2 + 2 + 4 + 1
	resp := consumer.parameters.client.coordinator.NewResponse()
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandUnsubscribe)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteByte(b, consumer.ID)
	err := consumer.parameters.client.HandleWrite(b.Bytes(), resp)
	consumer.response.code <- Code{id: CloseChannel}
	errC := consumer.parameters.client.coordinator.RemoveConsumerById(consumer.ID)
	if errC != nil {
		fmt.Printf("Errror %s", errC)
	}
	return err
}

func (c *Consumer) Commit() error {
	//public void commitOffset(String reference, String stream, long offset) {
	//	if (reference == null || reference.isEmpty() || reference.length() > 256) {
	//		throw new IllegalArgumentException(
	//			"Reference must a non-empty string of less than 256 characters");
	//	}
	//	if (stream == null || stream.isEmpty()) {
	//		throw new IllegalArgumentException("Stream cannot be null or empty");
	//	}
	length := 2 + 2 + 4 + 2 + len(c.parameters.consumerName) + 2 +
		len(c.parameters.streamName) + 8
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandCommitOffset)
	WriteShort(b, Version1)
	WriteInt(b, 0) // correlation ID not used yet, may be used if commit offset has a confirm
	WriteString(b, c.parameters.consumerName)
	WriteString(b, c.parameters.streamName)

	WriteLong(b, c.getOffset())
	return c.parameters.client.socket.writeAndFlush(b.Bytes())

}

func (consumer *Consumer) QueryOffset() (int64, error) {
	//if reference == null || reference.isEmpty() || reference.length() > 256 {
	//	throw
	//	new
	//	IllegalArgumentException(
	//		"Reference must a non-empty string of less than 256 characters")
	//}
	//if stream == null || stream.isEmpty() {
	//	throw
	//	new
	//	IllegalArgumentException("Stream cannot be null or empty")
	//}

	length := 2 + 2 + 4 + 2 + len(consumer.parameters.consumerName) + 2 + len(consumer.parameters.streamName)

	resp := consumer.parameters.client.coordinator.NewResponse()
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandQueryOffset)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)

	WriteString(b, consumer.parameters.consumerName)
	WriteString(b, consumer.parameters.streamName)
	err := consumer.parameters.client.HandleWriteWithResponse(b.Bytes(), resp, false)
	if err != nil {
		return 0, err

	}

	offset := <-resp.data
	_ = consumer.parameters.client.coordinator.RemoveResponseById(resp.correlationid)

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

	unusedOffset = int64(-1)
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
