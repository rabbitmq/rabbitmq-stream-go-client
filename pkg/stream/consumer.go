package stream

import (
	"context"
	"errors"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/constants"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"sync"
)

type MessagesHandler func(consumerContext ConsumerContext, message *amqp.Message)

type ConsumerContext struct {
	Consumer *Consumer
}

type Consumer struct {
	active          int
	mutex           *sync.Mutex
	Stream          string
	rawClient       raw.Clienter
	rawClientMu     *sync.Mutex
	opts            *ConsumerOptions
	MessagesHandler MessagesHandler
	chunkCh         chan *raw.Chunk
	closeCh         chan bool
	// The current status of the offset. Different from ConsumerOptionsOffset.
	currentOffset uint64
	// last stored offset to avoid storing the same value
	lastStoredOffset uint64
}

func NewConsumer(stream string, rawClient raw.Clienter, messagesHandler MessagesHandler, opts *ConsumerOptions, rawClientMu *sync.Mutex) (*Consumer, error) {
	if stream == "" {
		return nil, errors.New("stream name must not be empty")
	}

	if opts.InitialCredits > 1000 {
		return nil, errors.New("initial credits cannot be greater than 1000")
	}

	opts.validate()
	c := &Consumer{
		mutex:           &sync.Mutex{},
		rawClientMu:     rawClientMu,
		Stream:          stream,
		rawClient:       rawClient,
		opts:            opts,
		MessagesHandler: messagesHandler,
	}
	return c, nil
}

func (c *Consumer) getStatus() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.active
}

func (c *Consumer) setStatus(status int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.active = status
}

func (c *Consumer) Close(ctx context.Context) error {
	c.rawClientMu.Lock()
	defer c.rawClientMu.Unlock()
	return c.rawClient.Unsubscribe(ctx, c.opts.SubscriptionId)
}

func (c *Consumer) setCurrentOffset(offset uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.currentOffset = offset
}

func (c *Consumer) GetOffset() uint64 {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.currentOffset
}

func (c *Consumer) updateLastStoredOffset() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.lastStoredOffset < c.currentOffset {
		c.lastStoredOffset = c.currentOffset
		return true
	}
	return false
}

func (c *Consumer) GetLastStoredOffset() uint64 {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.lastStoredOffset
}

func (c *Consumer) decode() error {
	go func() {
		for {
			select {
			// get chunk from raw client
			case chunk := <-c.chunkCh:
				// store current offset
				c.setCurrentOffset(chunk.ChunkFirstOffset)
				// ToDo choose codec
				message := &amqp.Message{}
				err := message.UnmarshalBinary(chunk.Messages)
				if err != nil {
					fmt.Println("error parsing chunk", err)
				}
				// call  messages handler
				c.MessagesHandler(ConsumerContext{Consumer: c}, message)

			// Need someway to break and close
			case quit := <-c.closeCh:
				if quit {
					fmt.Println("go routing finished")
					return
				}
			}
		}
	}()

	return nil
}

type ConsumerOptions struct {
	Reference              string // consumer reference name, required for single active consumer
	SingleActiveConsumer   bool   // enable single active consumer
	ClientProvidedName     string // client provided name
	SuperStream            bool   // enable consuming messages from super stream
	OffsetType             uint16 // offset type see constants/types.go
	Offset                 uint64
	ConsumerUpdateListener func()      // rmq notifiies which consumer is active in single active consumer scenario
	InitialCredits         uint16      // Number of chunks consumer will receive at first
	Filter                 interface{} // consumer receives messages that match the filter
	Crc32                  interface{} // check crc on delivery when set
	SubscriptionId         uint8
}

func (co *ConsumerOptions) validate() {
	if co.OffsetType <= 0 {
		co.OffsetType = constants.OffsetTypeFirst // OffsetTypeFirst as default. Start consuming from the first message in the stream
	}
}
