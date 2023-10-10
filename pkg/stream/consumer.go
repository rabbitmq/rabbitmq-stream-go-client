package stream

import (
	"context"
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
		co.OffsetType = uint16(1) // OffsetTypeFirst as default. Start consuming from the first message in the stream
	}
}

type Consumer struct {
	Stream      string // stream name to consumer from
	rawClient   raw.Clienter
	rawClientMu *sync.Mutex
	opts        *ConsumerOptions
	//OffsetType     uint16 // offset type first, last, next, offset, timestamp
	//Offset         uint64 //
	//SubscriptionID uint8  // identifier in the server
	//Credit         uint16 //
}

func NewConsumer(stream string, rawClient raw.Clienter, opts *ConsumerOptions) *Consumer {
	opts.validate()
	c := &Consumer{
		Stream:    stream,
		rawClient: rawClient,
		opts:      opts,
	}
	return c
}

func (c *Consumer) Subscribe(ctx context.Context) error {
	subscribeProperties := map[string]string{}
	err := c.rawClient.Subscribe(ctx, c.Stream, c.opts.OffsetType, c.opts.SubscriptionId, c.opts.Credit, subscribeProperties, c.opts.Offset)
	if err != nil {
		return err
	}
	return nil
}
