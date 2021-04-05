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
