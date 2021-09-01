package ha

import (
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"sync"
	"sync/atomic"
	"time"
)

const (
	StatusOpen               = 1
	StatusClosed             = 2
	StatusStreamDoesNotExist = 3
)

func (r *ReliableProducer) handlePublishError(publishError stream.ChannelPublishError) {
	go func() {
		for {
			for err := range publishError {
				r.channelPublishError <- err
			}
		}
	}()

}

func (r *ReliableProducer) handlePublishConfirm(confirms stream.ChannelPublishConfirm) {
	go func() {
		for messagesIds := range confirms {
			atomic.AddInt32(&r.count, int32(len(messagesIds)))
			r.channelPublishConfirm <- messagesIds
		}
	}()
}

type ReliableProducer struct {
	env                   *stream.Environment
	producer              *stream.Producer
	streamName            string
	producerOptions       *stream.ProducerOptions
	channelPublishConfirm stream.ChannelPublishConfirm
	channelPublishError   stream.ChannelPublishError
	count                 int32

	mutex       *sync.Mutex
	mutexStatus *sync.Mutex
	status      int
	totalSent   int64
}

func NewHAProducer(env *stream.Environment, streamName string, producerOptions *stream.ProducerOptions) (*ReliableProducer, error) {
	res := &ReliableProducer{
		env:                   env,
		producer:              nil,
		status:                StatusClosed,
		streamName:            streamName,
		producerOptions:       producerOptions,
		mutex:                 &sync.Mutex{},
		mutexStatus:           &sync.Mutex{},
		channelPublishConfirm: make(chan []*stream.UnConfirmedMessage, 0),
		channelPublishError:   make(chan stream.PublishError, 0),
	}
	err := res.newProducer()
	if err == nil {
		res.setStatus(StatusOpen)
	}

	return res, err
}

func (p *ReliableProducer) newProducer() error {

	producer, err := p.env.NewProducer(p.streamName, p.producerOptions)
	if err != nil {
		return err
	}
	channelPublishError := producer.NotifyPublishError()
	p.handlePublishError(channelPublishError)
	channelPublishConfirm := producer.NotifyPublishConfirmation()
	p.handlePublishConfirm(channelPublishConfirm)
	p.producer = producer
	return err
}

func (p *ReliableProducer) NotifyPublishError() stream.ChannelPublishError {
	return p.channelPublishError
}

func (p *ReliableProducer) NotifyPublishConfirmation() stream.ChannelPublishConfirm {
	return p.channelPublishConfirm

}

func (p *ReliableProducer) Send(message message.StreamMessage) error {
	if p.getStatus() == StatusStreamDoesNotExist {
		return stream.StreamDoesNotExist
	}
	if p.getStatus() == StatusClosed {
		return errors.New("Producer is closed")
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()

	errW := p.producer.Send(message)
	p.totalSent += 1

	if errW != nil {
		switch errW {
		case stream.FrameTooLarge:
			{
				return stream.FrameTooLarge
			}
		default:
			logs.LogError("[RProducer] - error during send %s",errW.Error())
		}

	}
	// tls e non tls  connections have different error message
	if errW != nil {
		time.Sleep(200 * time.Millisecond)
		exists, errS := p.env.StreamExists(p.streamName)
		if errS != nil {
			return errS

		}
		if exists {
			logs.LogDebug("[RProducer] - stream %s exists. Reconnecting the producer.",p.streamName)
			time.Sleep(800 * time.Millisecond)
			p.producer.FlushUnConfirmedMessages()
			return p.newProducer()
		} else {
			logs.LogError("[RProducer] - stream %s does not exist. Closing..",p.streamName)
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
	close(p.channelPublishConfirm)
	close(p.channelPublishError)

	if err != nil {
		return err
	}
	return nil
}
