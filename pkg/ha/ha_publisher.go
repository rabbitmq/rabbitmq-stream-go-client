package ha

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"net"
	"sync"
	"time"
)

const (
	StatusOpen               = 1
	StatusClosed             = 2
	StatusStreamDoesNotExist = 3
)

type ReliableProducer struct {
	env            *stream.Environment
	producer       *stream.Producer
	status         int
	backoff        int
	streamName     string
	producerName   string
	mutex          *sync.Mutex
	mutexStatus    *sync.Mutex
	publishChannel chan []*amqp.Message
	totalSent      int64
}

func NewHAProducer(env *stream.Environment, streamName string, producerName string) (*ReliableProducer, error) {
	res := &ReliableProducer{
		env:            env,
		producer:       nil,
		status:         StatusClosed,
		backoff:        1,
		streamName:     streamName,
		producerName:   producerName,
		mutex:          &sync.Mutex{},
		mutexStatus:    &sync.Mutex{},
		publishChannel: make(chan []*amqp.Message, 1),
	}
	newProducer := res.newProducer()
	if newProducer == nil {
		res.setStatus(StatusOpen)
	}

	return res, newProducer
}

func (p *ReliableProducer) newProducer() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	producer, err := p.env.NewProducer(p.streamName, stream.NewProducerOptions().SetProducerName(p.producerName))
	if err != nil {
		return err
	}
	p.producer = producer
	return err
}

func (p *ReliableProducer) NotifyPublishError() stream.ChannelPublishError {
	return p.producer.NotifyPublishError()
}

func (p *ReliableProducer) NotifyPublishConfirmation() stream.ChannelPublishConfirm {
	return p.producer.NotifyPublishConfirmation()

}

func (p *ReliableProducer) BatchPublish(messages []*amqp.Message) error {
	if p.getStatus() == StatusStreamDoesNotExist {
		return stream.StreamDoesNotExist
	}
	if p.getStatus() == StatusClosed {
		return errors.New("Producer is closed")
	}

	p.mutex.Lock()
	_, errW := p.producer.BatchPublish(context.TODO(), messages)
	p.mutex.Unlock()
	p.totalSent += 1
	switch err := errW.(type) {
	case *net.OpError:
		fmt.Printf("cant %s \n", err.Err)
		time.Sleep(200 * time.Millisecond)
		exists, errS := p.env.StreamExists(p.streamName)
		if errS != nil {
			return errS

		}
		time.Sleep(100 * time.Millisecond)
		if exists {
			return p.newProducer()
		} else {
			return stream.StreamDoesNotExist
		}
	}

	return nil
}

func (p *ReliableProducer) IsOpen() bool {
	p.mutexStatus.Lock()
	defer p.mutexStatus.Unlock()
	return p.status == StatusOpen
}

func (p *ReliableProducer) getStatus() int {
	p.mutexStatus.Lock()
	defer p.mutexStatus.Unlock()
	return p.status
}

func (p *ReliableProducer) setStatus(value int) {
	p.mutexStatus.Lock()
	defer p.mutexStatus.Unlock()
	p.status = value
}

func (p *ReliableProducer) GetBroker() *stream.Broker {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.producer.GetBroker()
}

func (p *ReliableProducer) Close() error {
	p.setStatus(StatusClosed)
	err := p.producer.Close()
	if err != nil {
		return err
	}
	return nil
}
