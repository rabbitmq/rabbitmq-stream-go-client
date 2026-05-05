package ha

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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
	// currentPosition is updated atomically on every message delivery and read
	// without a lock in newConsumer(), so it must not be moved into mutexConnection's
	// critical section. See Issue-1 analysis: holding mutexConnection across the
	// blocking env.NewConsumer() call while the handler closure also needed that
	// same lock was the source of the stall.
	currentPosition atomic.Int64

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
				// Close() may have been called while retry() was running (backoff
				// can be many seconds). Use mutexStatus to atomically decide:
				// if the user already closed us, keep StatusClosed and clean up
				// the new consumer that newConsumer() just created.
				c.mutexStatus.Lock()
				alreadyClosed := c.status == StatusClosed
				if !alreadyClosed {
					c.status = StatusOpen
				}
				c.mutexStatus.Unlock()

				if alreadyClosed {
					c.mutexConnection.Lock()
					consumer := c.consumer
					c.mutexConnection.Unlock()
					if consumer != nil {
						_ = consumer.Close()
					}
					logs.LogInfo("[Reliable] - %s reconnected but was explicitly closed during reconnection. Closing new consumer.", c.getInfo())
				}
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
	// Read the current position atomically before the blocking subscribe call.
	// mutexConnection is NOT held here: env.NewConsumer() is a blocking network
	// operation (subscribe frame + server ack), and the message handler closure
	// registered below also needs mutexConnection for c.consumer access.
	// Holding the lock across that call created a stall where the consumer
	// dispatch goroutine (which runs the handler) was blocked on mutexConnection
	// while the read loop's sendChunk() could fill chunkForConsumer and block too.
	offset := stream.OffsetSpecification{}.Offset(c.currentPosition.Load() + 1)
	if c.bootstrap {
		offset = c.consumerOptions.Offset
	}
	logs.LogDebug("[Reliable] - creating %s. Boot: %s. StartOffset: %s", c.getInfo(),
		c.bootstrap, offset)
	consumer, err := c.env.NewConsumer(c.streamName, func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		// currentPosition is an atomic.Int64; no mutex required here.
		c.currentPosition.Store(consumerContext.Consumer.GetOffset())
		c.messagesHandler(consumerContext, message)
	}, c.consumerOptions.SetOffset(offset))
	if err != nil {
		return err
	}

	channelNotifyClose := consumer.NotifyClose()
	c.handleNotifyClose(channelNotifyClose)
	// Only lock briefly to publish the new consumer reference.
	c.mutexConnection.Lock()
	c.consumer = consumer
	c.mutexConnection.Unlock()
	return nil
}

func (c *ReliableConsumer) Close() error {
	c.setStatus(StatusClosed)
	// Snapshot the pointer under the lock to avoid a data race with newConsumer(),
	// which writes c.consumer under mutexConnection. The Close() call itself is
	// made outside the lock so we don't hold it across a blocking network operation.
	c.mutexConnection.Lock()
	consumer := c.consumer
	c.mutexConnection.Unlock()
	if consumer == nil {
		return nil
	}
	return consumer.Close()
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
