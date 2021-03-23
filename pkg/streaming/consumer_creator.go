package streaming

import (
	"bytes"
	"github.com/Azure/go-amqp"
)




type Consumer struct {
	ID       uint8
	client   *Client
	response *Response
}

type MessagesHandler func(consumerId uint8, message *amqp.Message)

type ConsumerCreator struct {
	client          *Client
	consumerName    string
	streamName      string
	messagesHandler MessagesHandler
}

func (client *Client) ConsumerCreator() *ConsumerCreator {
	return &ConsumerCreator{client: client}
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

func (c *ConsumerCreator) Build() (*Consumer, error) {
	consumer := c.client.consumers.New(c.client)
	length := 2 + 2 + 4 + 1 + 2 + len(c.streamName) + 2 + 2 // misses the offset
	//if (offsetSpecification.isOffset() || offsetSpecification.isTimestamp()) {
	//	length += 8;
	//}
	resp := c.client.responses.New()
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))

	WriteInt(b, length)
	WriteUShort(b, CommandSubscribe)
	WriteUShort(b, Version1)
	WriteInt(b, correlationId)
	WriteByte(b, consumer.ID)

	WriteString(b, c.streamName)

	WriteShort(b, 1)

	//	if (offsetSpecification.isOffset() || offsetSpecification.isTimestamp()) {
	//	WriteLong(offsetSpecification.getOffset());
	//}
	WriteShort(b, 10)

	res := c.client.HandleWrite(b.Bytes(), resp)

	go func() {
		for true {
			select {
			case code := <-consumer.response.code:
				if code.id == CloseChannel {
					_ = c.client.consumers.RemoveById(consumer.ID)
					return
				}

			case messages := <-consumer.response.data:
				c.messagesHandler(consumer.ID, messages.(*amqp.Message))
			}
		}
	}()

	return consumer, res

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
	client.socket.writeAndFlush(b.Bytes())
}

func (consumer *Consumer) UnSubscribe() error {
	return consumer.client.UnSubscribe(consumer.ID)
}
