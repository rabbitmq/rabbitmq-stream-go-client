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
	m                  sync.Mutex
	id                 int
	config             EnvironmentConfiguration
	connectionEndpoint raw.Broker
	producers          []internalProducer
	client             raw.Clienter
	clientM            *sync.Mutex
	open               bool
}

func newProducerManager(id int, config EnvironmentConfiguration) *producerManager {
	return &producerManager{
		m:         sync.Mutex{},
		id:        id,
		config:    config,
		producers: make([]internalProducer, 0, config.MaxProducersByConnection),
		client:    nil,
		clientM:   &sync.Mutex{},
		open:      false,
	}
}

func (p *producerManager) connect(ctx context.Context, uri string) error {
	conf, err := raw.NewClientConfiguration(uri)
	if err != nil {
		return err
	}
	conf.SetConnectionName(fmt.Sprintf("%s-%d", "rabbitmq-stream-producer", p.id))

	ctx2, cancel := maybeApplyDefaultTimeout(ctx)
	defer cancel()
	c, err := raw.DialConfig(ctx2, conf)
	if err != nil {
		return err
	}

	p.connectionEndpoint = conf.RabbitmqBroker()
	p.client = c
	p.open = true
	return nil
}

// initialises a producer and sends a declare publisher frame. It returns
// an error if the manager does not have capacity, or if declare publisher
// fails
func (p *producerManager) createProducer(ctx context.Context, producerOpts *ProducerOptions) (Producer, error) {
	if !p.open {
		return nil, errManagerClosed
	}

	var (
		publisherId uint8
		producer    *standardProducer
	)
	if len(p.producers) == 0 {
		publisherId = 0
		producer = newStandardProducer(publisherId, p.client, p.clientM, producerOpts)
		producer.setCloseCallback(p.removeProducer)
		p.producers = append(p.producers, producer)
	} else {
		for i := 0; i < len(p.producers); i++ {
			if p.producers[i] == nil {
				publisherId = uint8(i)
				producer = newStandardProducer(publisherId, p.client, p.clientM, producerOpts)
				p.producers[i] = producer
				break
			}
		}
		// no empty slots, manager at max capacity
		if producer == nil && len(p.producers) == p.config.MaxProducersByConnection {
			return nil, errManagerFull
		}
		// no empty slots, manager has capacity
		if producer == nil {
			publisherId = uint8(len(p.producers))
			producer = newStandardProducer(publisherId, p.client, p.clientM, producerOpts)
			p.producers = append(p.producers, producer)
		}
	}

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

func (p *producerManager) removeProducer(id int) error {
	if id >= len(p.producers) {
		return errIdOutOfBounds
	}
	if id < 0 {
		return errNegativeId
	}
	p.producers[id] = nil
	return nil
}
