package stream

import (
	"context"
	"errors"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"sync"
)

var (
	errManagerFull   = errors.New("producer manager is full")
	errManagerClosed = errors.New("producer manager is closed")
	errNegativeId    = errors.New("negative id")
	errIdOutOfBounds = errors.New("id outbounds producer list")
)

const noReference = ""

type producerManager struct {
	m      sync.Mutex
	id     int
	config EnvironmentConfiguration
	// records the endpoint that this manager is connected to
	connectionEndpoint   raw.RabbitmqAddress
	producers            []internalProducer
	confirmationChannels []chan *publishConfirmOrError
	// keeps track of how many producers are assigned to this manager
	producerCount int
	client        raw.Clienter
	clientM       *sync.Mutex
	open          bool
}

func newProducerManager(id int, config EnvironmentConfiguration) *producerManager {
	return &producerManager{
		m:             sync.Mutex{},
		id:            id,
		config:        config,
		producers:     make([]internalProducer, config.MaxProducersByConnection),
		producerCount: 0,
		client:        nil,
		clientM:       &sync.Mutex{},
		open:          false,
	}
}

func newProducerManagerWithClient(id int, config EnvironmentConfiguration, client raw.Clienter, rc *raw.ClientConfiguration) (manager *producerManager) {
	manager = newProducerManager(id, config)
	manager.client = client
	manager.open = true
	manager.connectionEndpoint = rc.RabbitmqAddr
	return manager
}

func (p *producerManager) connect(ctx context.Context, uri string) error {
	conf, err := raw.NewClientConfiguration(uri)
	if err != nil {
		return err
	}
	conf.ConnectionName = fmt.Sprintf("%s-%d", "rabbitmq-stream-producer", p.id)

	ctx2, cancel := maybeApplyDefaultTimeout(ctx)
	defer cancel()
	c, err := raw.DialConfig(ctx2, conf)
	if err != nil {
		return err
	}

	p.connectionEndpoint = conf.RabbitmqAddr
	p.client = c
	p.open = true

	//confirmCh := c.NotifyPublish(make(chan *raw.PublishConfirm, DefaultMaxInFlight))

	return nil
}

func (p *producerManager) publishConfirmationListener(ctx context.Context, c chan *raw.PublishConfirm) {
	panic("implement me!")
}

// initialises a producer and sends a declare publisher frame. It returns
// an error if the manager does not have capacity, or if declare publisher
// fails
func (p *producerManager) createProducer(ctx context.Context, producerOpts *ProducerOptions) (Producer, error) {
	if !p.open {
		return nil, errManagerClosed
	}

	// TODO: optimisation - check if producerCount == len(producers) -> return errManagerFull

	var (
		publisherId uint8
		producer    *standardProducer
	)
	for i := 0; i < len(p.producers); i++ {
		if p.producers[i] == nil {
			publisherId = uint8(i)
			producer = newStandardProducer(publisherId, p.client, p.clientM, producerOpts)
			p.producers[i] = producer
			p.producerCount += 1
			break
		}
	}
	// no empty slots, manager at max capacity
	if producer == nil {
		return nil, errManagerFull
	}

	producer.setCloseCallback(p.removeProducer)

	ctx2, cancel := maybeApplyDefaultTimeout(ctx)
	defer cancel()
	p.clientM.Lock()
	defer p.clientM.Unlock()
	err := p.client.DeclarePublisher(ctx2, publisherId, noReference, producerOpts.stream)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

/*
Removes a producer with index == id

It closes the connection and sets the manager state to closed if the last
producer is removed.
*/
func (p *producerManager) removeProducer(id int) error {
	if id >= len(p.producers) {
		return errIdOutOfBounds
	}
	if id < 0 {
		return errNegativeId
	}
	p.producers[id] = nil
	p.producerCount -= 1

	if p.producerCount == 0 {
		// last producer in this manager, closing connection
		p.open = false
		p.clientM.Lock()
		defer p.clientM.Unlock()
		ctx, cancel := maybeApplyDefaultTimeout(context.Background())
		defer cancel()
		_ = p.client.Close(ctx)
		// FIXME: have a logger and log error
	}

	return nil
}
