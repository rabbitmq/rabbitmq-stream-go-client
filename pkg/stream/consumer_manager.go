package stream

import (
	"context"
	"errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"sync"
)

// Consumer Manager creates the channel to receive messages on and initializes consumers for all connection scenarios
type consumerManager struct {
	m      sync.Mutex
	config EnvironmentConfiguration
	opts   ConsumerOptions
	Stream string

	consumers     []*Consumer
	consumerCount int

	client   raw.Clienter
	clientMu *sync.Mutex
	chunkCh  <-chan *raw.Chunk
	open     bool
}

func newConsumerManager(config EnvironmentConfiguration, rawClient raw.Clienter) *consumerManager {
	consumerManager := &consumerManager{
		m:        sync.Mutex{},
		config:   config,
		client:   rawClient,
		clientMu: &sync.Mutex{},
		open:     false,
	}

	consumerManager.createNotifyChannel()

	// start dispatcher in seperate goroutine
	go consumerManager.dispatcher()

	return consumerManager
}

// dispatcher listens for incoming messages and distributes them across the consumers
func (c *consumerManager) dispatcher() {
	for {
		msg := <-c.chunkCh
		consumers := len(c.consumers)
		for i := 0; i < consumers; i++ {
			c.consumers[i].chunkCh <- msg
		}
	}
}

func (c *consumerManager) createNotifyChannel() {
	// only setup channel for consumers to receive messages if it does not exist
	if !c.open {
		c.open = true
		// create a channel, set it on the raw layer via Notify Chunk and set for the consumer
		c.chunkCh = c.client.NotifyChunk(make(chan *raw.Chunk))
	}
}

// initialize a consumer and establishes a channel to receive messages
func (c *consumerManager) createConsumer(stream string, messagesHandler MessagesHandler, opts *ConsumerOptions) error {
	var (
		consumer *Consumer
		err      error
	)

	// check len consumers is not equal or greater than max consumers
	if len(c.consumers) < c.config.MaxConsumersByConnection {
		consumer, err = NewConsumer(stream, c.client, messagesHandler, opts, c.clientMu)
		if err != nil {
			return err
		}
		consumer.rawClient = c.client
		c.consumers = append(c.consumers, consumer)
		c.consumerCount += 1
	} else {
		return errors.New("consumer manager is full")
	}

	return nil
}

func (c *consumerManager) createMaxConsumers(stream string, messagesHandler MessagesHandler, opts *ConsumerOptions) error {
	for i := 0; i < c.config.MaxConsumersByConnection; i++ {
		err := c.createConsumer(stream, messagesHandler, opts)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *consumerManager) subscribeProperties() raw.SubscribeProperties {
	s := raw.SubscribeProperties{}

	if c.opts.SingleActiveConsumer {
		s["single-active-consumer"] = "true"
	}
	if c.opts.SuperStream {
		s["super-stream"] = c.Stream
	}
	return s
}

func (c *consumerManager) Subscribe(ctx context.Context) error {
	// Call raw client subscribe so the chunkCh can start receiving messages message handling is done in the consumers
	c.clientMu.Lock()
	defer c.clientMu.Unlock()

	subscribeProperties := c.subscribeProperties()

	err := c.client.Subscribe(ctx, c.Stream, c.opts.OffsetType, c.opts.SubscriptionId, c.opts.InitialCredits, subscribeProperties, c.opts.Offset)
	if err != nil {
		return err
	}

	return nil
}
