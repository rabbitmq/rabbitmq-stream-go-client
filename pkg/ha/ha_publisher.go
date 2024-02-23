package ha

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	StatusOpen               = 1
	StatusClosed             = 2
	StatusStreamDoesNotExist = 3
	StatusReconnecting       = 4
)

func (p *ReliableProducer) handlePublishConfirm(confirms stream.ChannelPublishConfirm) {
	go func() {
		for messagesIds := range confirms {
			atomic.AddInt32(&p.count, int32(len(messagesIds)))
			p.confirmMessageHandler(messagesIds)
		}
	}()
}

func (p *ReliableProducer) handleNotifyClose(channelClose stream.ChannelClose) {
	go func() {
		for event := range channelClose {
			// TODO: Convert the string to a constant
			if event.Reason == "socket client closed" {
				logs.LogError("[RProducer] - producer closed unexpectedly.. Reconnecting..")
				err, reconnected := p.retry()
				if err != nil {
					// TODO: Handle stream is not available
					return
				}
				if reconnected {
					p.setStatus(StatusOpen)
				}
				p.reconnectionSignal <- struct{}{}
			}
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
	reconnectionSignal    chan struct{}
}

type ConfirmMessageHandler func(messageConfirm []*stream.ConfirmationStatus)

func NewReliableProducer(env *stream.Environment, streamName string,
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
		reconnectionSignal:    make(chan struct{}),
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
	channelPublishConfirm := producer.NotifyPublishConfirmation()
	channelNotifyClose := producer.NotifyClose()
	p.handleNotifyClose(channelNotifyClose)
	p.handlePublishConfirm(channelPublishConfirm)
	p.producer = producer
	return err
}

func (p *ReliableProducer) Send(message message.StreamMessage) error {
	if p.getStatus() == StatusStreamDoesNotExist {
		return stream.StreamDoesNotExist
	}
	if p.getStatus() == StatusClosed {
		return errors.New("producer is closed")
	}

	if p.getStatus() == StatusReconnecting {
		logs.LogDebug("[RProducer] - producer is reconnecting")
		<-p.reconnectionSignal
		logs.LogDebug("[RProducer] - producer reconnected")
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	errW := p.producer.Send(message)

	if errW != nil {
		switch {
		case errors.Is(errW, stream.FrameTooLarge):
			{
				return stream.FrameTooLarge
			}
		default:
			logs.LogError("[RProducer] - error during send %s", errW.Error())
		}

	}

	return nil
}

func (p *ReliableProducer) retry() (error, bool) {
	p.setStatus(StatusReconnecting)
	sleepValue := rand.Intn(int(p.producerOptions.ConfirmationTimeOut.Seconds()) - 2)
	time.Sleep(time.Duration(sleepValue) * time.Second)
	exists, errS := p.env.StreamExists(p.streamName)
	if errS != nil {
		return errS, true

	}
	if exists {
		logs.LogDebug("[RProducer] - stream %s exists. Reconnecting the producer.", p.streamName)
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
	err := p.producer.Close()
	if err != nil {
		return err
	}
	return nil
}
