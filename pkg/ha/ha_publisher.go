package ha

import (
	"errors"
	"fmt"
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
				err, reconnected := p.retry(1)
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

func (p *ReliableProducer) retry(backoff int) (error, bool) {
	p.setStatus(StatusReconnecting)
	sleepValue := rand.Intn(int((p.producerOptions.ConfirmationTimeOut.Seconds()-2+1)+2)*1000) + backoff*1000
	logs.LogInfo("[RProducer] - The producer for the stream %s is in reconnection in %d milliseconds", p.streamName, sleepValue)
	time.Sleep(time.Duration(sleepValue) * time.Millisecond)
	streamMetaData, errS := p.env.StreamMetaData(p.streamName)
	if errors.Is(errS, stream.StreamDoesNotExist) {
		return errS, true
	}
	if errors.Is(errS, stream.StreamNotAvailable) {
		logs.LogInfo("[RProducer] - stream %s is not available. Trying to reconnect", p.streamName)
		return p.retry(backoff + 1)
	}
	if streamMetaData.Leader == nil {
		logs.LogInfo("[RProducer] - The leader for the stream %s is not ready. Trying to reconnect")
		return p.retry(backoff + 1)
	}

	var result error
	if streamMetaData != nil {
		logs.LogInfo("[RProducer] - stream %s exists. Reconnecting the producer.", p.streamName)
		result = p.newProducer()
		if result == nil {
			logs.LogInfo("[RProducer] - stream %s exists. Producer reconnected.", p.streamName)
		} else {
			logs.LogInfo("[RProducer] - error creating producer for the stream %s exists. Trying to reconnect", p.streamName)
			return p.retry(backoff + 1)
		}
	} else {
		logs.LogError("[RProducer] - stream %s does not exist. Closing..", p.streamName)
		result = stream.StreamDoesNotExist
	}

	return result, true

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
