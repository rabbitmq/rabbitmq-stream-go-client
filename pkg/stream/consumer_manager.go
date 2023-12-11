package stream

import (
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"sync"
)

// Consumer Manager creates the channel to receive messages on and initializes consumers for all connection scenarios
type consumerManager struct {
	m      sync.Mutex
	config EnvironmentConfiguration

	consumers     []*Consumer
	consumerCount int

	client   raw.Clienter
	clientMu *sync.Mutex
	chunkCh  <-chan *raw.Chunk
	open     bool
}

func newConsumerManager(config EnvironmentConfiguration) *consumerManager {
	return &consumerManager{
		m:        sync.Mutex{},
		config:   config,
		client:   nil,
		clientMu: &sync.Mutex{},
		open:     false,
	}
}

// initialize a consumer and establishes a channel to receive messages
func (c *consumerManager) createConsumer(stream string, messagesHandler MessagesHandler, opts *ConsumerOptions) (*Consumer, error) {
	var (
		consumer *Consumer
		err      error
	)

	// check len consumers is not equal or greater than max consumers
	if len(c.consumers) < c.config.MaxConsumersByConnection {
		consumer, err = NewConsumer(stream, c.client, messagesHandler, opts, c.clientMu)
		if err != nil {
			return nil, err
		}
		c.consumers = append(c.consumers, consumer)
		c.consumerCount += 1
	}

	// only setup channel for consumers to receive messages if it does not exist
	if !c.open {
		c.open = true
		// create a channel, set it on the raw layer via Notify Chunk and set for the consumer
		c.chunkCh = c.client.NotifyChunk(make(chan *raw.Chunk))
		consumer.chunkCh = c.chunkCh
	}

	return consumer, nil
}
