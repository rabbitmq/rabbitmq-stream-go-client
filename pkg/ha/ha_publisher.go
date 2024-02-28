package ha

import (
	"errors"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"sync"
	"sync/atomic"
	"time"
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
			if event.Reason == stream.SocketCloseError {
				logs.LogError("[RProducer] - producer closed unexpectedly.. Reconnecting..")
				err, reconnected := retry(0, p)
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
		logs.LogDebug("[RProducer] - send producer is reconnecting")
		<-p.reconnectionSignal
		logs.LogDebug("[RProducer] - send producer reconnected")
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
			time.Sleep(500 * time.Millisecond)
			logs.LogError("[RProducer] - error during send %s", errW.Error())
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

// Reliabler interface
func (p *ReliableProducer) setStatus(value int) {
	p.mutexStatus.Lock()
	defer p.mutexStatus.Unlock()
	p.status = value
}

func (p *ReliableProducer) getInfo() string {
	return fmt.Sprintf("producer %s for stream %s",
		p.producerOptions.ClientProvidedName, p.streamName)
}

func (p *ReliableProducer) getEnv() *stream.Environment {
	return p.env
}

func (p *ReliableProducer) getNewInstance() newEntityInstance {
	return p.newProducer
}

func (p *ReliableProducer) getTimeOut() time.Duration {
	return p.producerOptions.ConfirmationTimeOut
}

func (p *ReliableProducer) getStreamName() string {
	return p.streamName
}

// End of Reliabler interface

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
