package ha

import (
	"fmt"
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

func (p *ReliableProducer) handlePublishError(publishError stream.ChannelPublishError) {
	go func() {
		for {
			for err := range publishError {
				errMessage := err.UnConfirmedMessage
				errMessage.Err = err.Err
				errMessage.Confirmed = false
				p.confirmMessageHandler([]*stream.UnConfirmedMessage{errMessage})
			}
		}
	}()
}

func (p *ReliableProducer) handlePublishConfirm(confirms stream.ChannelPublishConfirm) {
	go func() {
		for messagesIds := range confirms {
			atomic.AddInt32(&p.count, int32(len(messagesIds)))
			p.confirmMessageHandler(messagesIds)
		}
	}()
}

type ReliableProducer struct {
	env                   *stream.Environment
	producer              *stream.Producer
	streamName            string
	producerOptions       *stream.ProducerOptions
	count                 int32
	confirmMessageHandler ConfirmMessageHandler
	mutex                 *sync.Mutex
	mutexStatus           *sync.Mutex
	status                int
}

type ConfirmMessageHandler func(messageConfirm []*stream.UnConfirmedMessage)

func NewHAProducer(env *stream.Environment, streamName string,
	producerOptions *stream.ProducerOptions,
	confirmMessageHandler ConfirmMessageHandler) (*ReliableProducer, error) {
	res := &ReliableProducer{
		env:                   env,
		producer:              nil,
		status:                StatusClosed,
		streamName:            streamName,
		producerOptions:       producerOptions,
		mutex:                 &sync.Mutex{},
		mutexStatus:           &sync.Mutex{},
		confirmMessageHandler: confirmMessageHandler,
	}
	if confirmMessageHandler == nil {
		return nil, fmt.Errorf("the confirmation message handler is mandatory")
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

	if errW != nil {
		switch errW {
		case stream.FrameTooLarge:
			{
				return stream.FrameTooLarge
			}
		default:
			logs.LogError("[RProducer] - error during send %s", errW.Error())
		}

	}

	if errW != nil {
		err, done := p.retry()
		if done {
			return err
		}
	}

	return nil
}


func (p *ReliableProducer) BatchSend(messages []message.StreamMessage) error {
	if p.getStatus() == StatusStreamDoesNotExist {
		return stream.StreamDoesNotExist
	}
	if p.getStatus() == StatusClosed {
		return errors.New("Producer is closed")
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()

	errW := p.producer.BatchSend(messages)

	if errW != nil {
		switch errW {
		case stream.FrameTooLarge:
			{
				return stream.FrameTooLarge
			}
		default:
			logs.LogError("[RProducer] - error during send %s", errW.Error())
		}

	}

	if errW != nil {
		err, done := p.retry()
		if done {
			return err
		}
	}

	return nil
}

func (p *ReliableProducer) retry() (error, bool) {
	time.Sleep(200 * time.Millisecond)
	exists, errS := p.env.StreamExists(p.streamName)
	if errS != nil {
		return errS, true

	}
	if exists {
		logs.LogDebug("[RProducer] - stream %s exists. Reconnecting the producer.", p.streamName)
		p.producer.FlushUnConfirmedMessages()
		return p.newProducer(), true
	} else {
		logs.LogError("[RProducer] - stream %s does not exist. Closing..", p.streamName)
		return stream.StreamDoesNotExist, true
	}
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
	p.producer.FlushUnConfirmedMessages()
	err := p.producer.Close()
	if err != nil {
		return err
	}
	return nil
}
