package stream

import (
	"bytes"
	"github.com/Azure/go-amqp"
)

type Handler interface {
	Messages(message *amqp.Message)
}

type Consumer struct {
	ID          uint8
	response    *Response
	LikedClient *Client
}

func (client *Client) NewConsumer(stream string, m func(subscriberId byte, message *amqp.Message)) (*Consumer, error) {

	return client.declareConsumer(stream, m)
}

func (client *Client) declareConsumer(stream string, m func(subscriberId byte, message *amqp.Message)) (*Consumer, error) {
	consumer := client.consumers.New(client)
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

	_, err := WaitCodeWithDefaultTimeOut(resp, CommandSubscribe)
	if err != nil {
		return nil, err
	}
	err = client.responses.RemoveById(correlationId)
	if err != nil {
		return nil, err
	}

	go func() {
		for true {
			select {
			case code := <-consumer.response.code:
				if code.id == CloseChannel {
					_ = client.consumers.RemoveById(consumer.ID)
					return
				}

			case messages := <-consumer.response.data:
				m(consumer.ID, messages.(*amqp.Message))
			}
		}
	}()

	return consumer, nil
}

func (client *Client) credit(subscriptionId byte, credit int16) {
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
	client.writeAndFlush(b.Bytes())
}

func (consumer *Consumer) UnSubscribe() error {
	return consumer.LikedClient.UnSubscribe(consumer.ID)
}
