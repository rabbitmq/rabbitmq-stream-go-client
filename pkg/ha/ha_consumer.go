package ha

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

// ReliableConsumer is a consumer that can reconnect in case of connection problems
// the function messagesHandler is mandatory
type ReliableConsumer struct {
	env             *stream.Environment
	consumer        *stream.Consumer
	streamName      string
	consumerOptions *stream.ConsumerOptions
	mutexStatus     *sync.Mutex
	mutexConnection *sync.Mutex
	status          int
	messagesHandler stream.MessagesHandler
	currentPosition int64 // the last offset consumed. It is needed in case of restart

	//bootstrap: if true the consumer will start from the user offset.
	// If false it will start from the last offset consumed (currentPosition)
	bootstrap bool
}

func (c *ReliableConsumer) GetStatusAsString() string {
	return getStatusAsString(c)
}

func (c *ReliableConsumer) handleNotifyClose(channelClose stream.ChannelClose) {
	go func() {
		event := <-channelClose
		if strings.EqualFold(event.Reason, stream.SocketClosed) || strings.EqualFold(event.Reason, stream.MetaDataUpdate) || strings.EqualFold(event.Reason, stream.ZombieConsumer) {
			c.setStatus(StatusReconnecting)
			logs.LogWarn("[Reliable] - %s closed unexpectedly %s.. Reconnecting..", c.getInfo(), event.Reason)
			c.bootstrap = false
			err, reconnected := retry(1, c, c.GetStreamName())
			if err != nil {
				logs.LogInfo(""+
					"[Reliable] - %s won't be reconnected. Error: %s", c.getInfo(), err)
			}
			if reconnected {
				c.setStatus(StatusOpen)
			} else {
				c.setStatus(StatusClosed)
			}
		} else {
			logs.LogInfo("[Reliable] - %s closed normally. Reason: %s", c.getInfo(), event.Reason)
			c.setStatus(StatusClosed)
		}
	}()
}

func NewReliableConsumer(env *stream.Environment, streamName string,
	consumerOptions *stream.ConsumerOptions, messagesHandler stream.MessagesHandler) (*ReliableConsumer, error) {
	res := &ReliableConsumer{
		env:             env,
		streamName:      streamName,
		consumerOptions: consumerOptions,
		mutexStatus:     &sync.Mutex{},
		mutexConnection: &sync.Mutex{},
		messagesHandler: messagesHandler,
		currentPosition: 0,
		bootstrap:       true,
	}
	if messagesHandler == nil {
		return nil, fmt.Errorf("the messages handler is mandatory")
	}
	if consumerOptions == nil {
		return nil, fmt.Errorf("the consumer options is mandatory")
	}
	logs.LogDebug("[Reliable] - creating %s", res.getInfo())
	err := res.newConsumer()
	if err == nil {
		res.setStatus(StatusOpen)
	}
	logs.LogDebug("[Reliable] - created %s", res.getInfo())
	return res, err
}

func (c *ReliableConsumer) setStatus(value int) {
	c.mutexStatus.Lock()
	defer c.mutexStatus.Unlock()
	c.status = value
}

func (c *ReliableConsumer) GetStatus() int {
	c.mutexStatus.Lock()
	defer c.mutexStatus.Unlock()
	return c.status
}

func (c *ReliableConsumer) getEnv() *stream.Environment {
	return c.env
}

func (c *ReliableConsumer) GetStreamName() string {
	return c.streamName
}

func (c *ReliableConsumer) getNewInstance(_ string) newEntityInstance {
	return c.newConsumer
}

func (c *ReliableConsumer) getInfo() string {
	return fmt.Sprintf("consumer %s for stream %s",
		c.consumerOptions.ClientProvidedName, c.streamName)
}

func (c *ReliableConsumer) getTimeOut() time.Duration {
	return time.Duration(3)
}

func (c *ReliableConsumer) newConsumer() error {
	c.mutexConnection.Lock()
	defer c.mutexConnection.Unlock()
	offset := stream.OffsetSpecification{}.Offset(c.currentPosition + 1)
	if c.bootstrap {
		offset = c.consumerOptions.Offset
	}
	logs.LogDebug("[Reliable] - creating %s. Boot: %s. StartOffset: %s", c.getInfo(),
		c.bootstrap, offset)
	consumer, err := c.env.NewConsumer(c.streamName, func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		c.mutexConnection.Lock()
		c.currentPosition = consumerContext.Consumer.GetOffset()
		c.mutexConnection.Unlock()

		c.messagesHandler(consumerContext, message)
	}, c.consumerOptions.SetOffset(offset))
	if err != nil {
		return err
	}

	channelNotifyClose := consumer.NotifyClose()
	c.handleNotifyClose(channelNotifyClose)
	c.consumer = consumer
	return err
}

func (c *ReliableConsumer) Close() error {
	c.setStatus(StatusClosed)
	err := c.consumer.Close()
	if err != nil {
		return err
	}
	return nil
}

func (c *ReliableConsumer) GetInfo() string {
	return c.getInfo()
}

// Deprecated: see consumer.GetLastStoredOffset()
// use QueryOffset() instead
func (c *ReliableConsumer) GetLastStoredOffset() int64 {
	c.mutexConnection.Lock()
	defer c.mutexConnection.Unlock()

	return c.consumer.GetLastStoredOffset()
}

// QueryOffset returns the last stored offset for this consumer given its name and stream
func (c *ReliableConsumer) QueryOffset() (int64, error) {
	c.mutexConnection.Lock()
	defer c.mutexConnection.Unlock()
	return c.consumer.QueryOffset()
}

// StoreOffset stores the current offset for this consumer given its name and stream
func (c *ReliableConsumer) StoreOffset() error {
	c.mutexConnection.Lock()
	defer c.mutexConnection.Unlock()
	return c.consumer.StoreOffset()
}

// StoreCustomOffset stores a custom offset for this consumer given its name and stream
func (c *ReliableConsumer) StoreCustomOffset(offset int64) error {
	c.mutexConnection.Lock()
	defer c.mutexConnection.Unlock()

	return c.consumer.StoreCustomOffset(offset)
}
