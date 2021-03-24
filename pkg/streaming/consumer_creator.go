package streaming

import (
	"bytes"
	"github.com/Azure/go-amqp"
)

type Consumer struct {
	ID       uint8
	response *Response
	parameters *ConsumerCreator
}

type MessagesHandler func(consumerId uint8, message *amqp.Message)

type ConsumerCreator struct {
	client              *Client
	consumerName        string
	streamName          string
	messagesHandler     MessagesHandler
	offsetSpecification OffsetSpecification
}

func (c *Client) ConsumerCreator() *ConsumerCreator {
	return &ConsumerCreator{client: c, offsetSpecification: OffsetSpecification{}.First()}
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

func (c *ConsumerCreator) Offset(offsetSpecification OffsetSpecification) *ConsumerCreator {
	c.offsetSpecification = offsetSpecification
	return c
}

func (c *ConsumerCreator) Build() (*Consumer, error) {
	consumer := c.client.consumers.New(c)
	length := 2 + 2 + 4 + 1 + 2 + len(c.streamName) + 2 + 2
	//|| offsetSpecification.isTimestamp()
	if c.offsetSpecification.isOffset() {
		length += 8
	}
	resp := c.client.responses.New()
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))

	WriteInt(b, length)
	WriteUShort(b, CommandSubscribe)
	WriteUShort(b, Version1)
	WriteInt(b, correlationId)
	WriteByte(b, consumer.ID)

	WriteString(b, c.streamName)

	WriteShort(b, c.offsetSpecification.typeOfs)
	//|| offsetSpecification.isTimestamp()
	if c.offsetSpecification.isOffset() {
		WriteLong(b, c.offsetSpecification.offset)
	}
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
	return consumer.parameters.client.UnSubscribe(consumer.ID)
}

/*
Offset constants
*/
const (
	typeFirst     = int16(1)
	typeLast      = int16(2)
	typeNext      = int16(3)
	typeOffset    = int16(4)
	typeTimestamp = int16(5)
	unusedOffset  = int64(-1)
)

type OffsetSpecification struct {
	typeOfs int16
	offset  int64
}

func (o OffsetSpecification) First() OffsetSpecification {
	o.typeOfs = typeFirst
	return o
}

func (o OffsetSpecification) Offset(offset int64) OffsetSpecification {
	o.typeOfs = typeOffset
	o.offset = offset
	return o
}

func (o OffsetSpecification) isOffset() bool {
	return o.typeOfs == typeOffset
}

//func (offset Offset) first()  {
//	return FIRST;
//}
//public static Offset first() {
//return FIRST;
//}
//
//public static Offset last() {
//return LAST;
//}
//
//public static Offset next() {
//return NEXT;
//}

/*
public class Offset {

  private static final short TYPE_FIRST = 1;
  private static final short TYPE_LAST = 2;
  private static final short TYPE_NEXT = 3;
  private static final short TYPE_OFFSET = 4;
  private static final short TYPE_TIMESTAMP = 5;

  private static final long UNUSED_OFFSET = -1;

  private static final Offset FIRST =
      new Offset(TYPE_FIRST, UNUSED_OFFSET);
  private static final Offset LAST = new Offset(TYPE_LAST, UNUSED_OFFSET);
  private static final Offset NEXT = new Offset(TYPE_NEXT, UNUSED_OFFSET);

  private final short type;
  private final long offset;

  private Offset(short type, long offset) {
    this.type = type;
    this.offset = offset;
  }

  public static Offset first() {
    return FIRST;
  }

  public static Offset last() {
    return LAST;
  }

  public static Offset next() {
    return NEXT;
  }

  public static Offset offset(long offset) {
    return new Offset(TYPE_OFFSET, offset);
  }

  public static Offset timestamp(long timestamp) {
    return new Offset(TYPE_TIMESTAMP, timestamp);
  }

  public boolean isOffset() {
    return this.type == TYPE_OFFSET;
  }

  public boolean isTimestamp() {
    return this.type == TYPE_TIMESTAMP;
  }

  public short getType() {
    return type;
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public String toString() {
    return "Offset{" + "type=" + type + ", offset=" + offset + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Offset that = (Offset) o;
    return type == that.type && offset == that.offset;
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, offset);
  }
}

*/
