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

type ConsumerOptions struct {
	Reference              string // consumer reference name, required for single active consumer
	SingleActiveConsumer   bool   // enable single active consumer
	ClientProvidedName     string // client provided name
	Callback               func() // callback function where consumer receives messages
	SuperStream            bool   // enable consuming messages from super stream
	OffsetType             uint16 // offset type see constants/types.go
	Offset                 uint64
	ConsumerUpdateListener func()      // rmq notifiies which consumer is active in single active consumer scenario
	InitialCredits         uint16      // Number of chunks consumer will receive at first
	Filter                 interface{} // consumer receives messages that match the filter
	Crc32                  interface{} // check crc on delivery when set
	SubscriptionId         uint8
	Credit                 uint16
}

func (co *ConsumerOptions) validate() {
	if co.OffsetType <= 0 {
		co.OffsetType = constants.OffsetTypeFirst // OffsetTypeFirst as default. Start consuming from the first message in the stream
	}
}

type MessagesHandler func(context context.Context, message *amqp.Message)

type Consumer struct {
	Stream          string
	rawClient       raw.Clienter
	rawClientMu     *sync.Mutex
	opts            *ConsumerOptions
	MessagesHandler MessagesHandler
}

func NewConsumer(stream string, rawClient raw.Clienter, messagesHandler MessagesHandler, opts *ConsumerOptions) (*Consumer, error) {
	if stream == "" {
		return nil, errors.New("stream name must not be empty")
	}
	opts.validate()
	c := &Consumer{
		Stream:          stream,
		rawClient:       rawClient,
		opts:            opts,
		MessagesHandler: messagesHandler,
	}
	return c, nil
}

func (c *Consumer) Subscribe(ctx context.Context) error {
	subscribeProperties := map[string]string{}

	//declare consumer
	err := c.rawClient.Subscribe(ctx, c.Stream, c.opts.OffsetType, c.opts.SubscriptionId, c.opts.Credit, subscribeProperties, c.opts.Offset)
	if err != nil {
		return err
	}

	// get messages
	// call raw client notify chunk, when chunk appears on the channel, parse it and call the messages handler with the
	// parsed messaage
	chunkChan := make(chan *raw.Chunk)
	messagesChan := c.rawClient.NotifyChunk(chunkChan)

	chunk, ok := <-messagesChan
	if ok {
		fmt.Println("Channel is open!")
	} else {
		fmt.Println("Channel is closed!")
	}

	message := &amqp.Message{}
	err = message.UnmarshalBinary(chunk.Messages)
	if err != nil {
		//log err
	}
	c.MessagesHandler(ctx, message)

	return nil
}

//Todo NotifyChunk

func (c *Consumer) Close(ctx context.Context) error {
	return c.rawClient.Unsubscribe(ctx, c.opts.SubscriptionId)
}
