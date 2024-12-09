package ha

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
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
			if strings.EqualFold(event.Reason, stream.SocketClosed) || strings.EqualFold(event.Reason, stream.MetaDataUpdate) {
				logs.LogWarn("[Reliable] - %s closed unexpectedly.. Reconnecting..", p.getInfo())
				err, reconnected := retry(0, p)
				if err != nil {
					logs.LogInfo(""+
						"[Reliable] - %s won't be reconnected. Error: %s", p.getInfo(), err)
				}
				if reconnected {
					p.setStatus(StatusOpen)
				} else {
					p.setStatus(StatusClosed)
				}
			} else {
				logs.LogInfo("[Reliable] - %s closed normally. Reason: %s", p.getInfo(), event.Reason)
				p.setStatus(StatusClosed)
			}

			p.reconnectionSignal.L.Lock()
			p.reconnectionSignal.Broadcast()
			p.reconnectionSignal.L.Unlock()
		}
	}()
}

// ReliableProducer is a producer that can reconnect in case of connection problems
// the function handlePublishConfirm is mandatory
// in case of problems the messages have the message.Confirmed == false
// The functions `Send` and `SendBatch` are blocked during the reconnection
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
	reconnectionSignal    *sync.Cond
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
		reconnectionSignal:    sync.NewCond(&sync.Mutex{}),
	}
	if confirmMessageHandler == nil {
		return nil, fmt.Errorf("the confirmation message handler is mandatory")
	}
	if producerOptions == nil {
		return nil, fmt.Errorf("the producer options is mandatory")
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

func (p *ReliableProducer) isReadyToSend() error {
	if p.GetStatus() == StatusStreamDoesNotExist {
		return stream.StreamDoesNotExist
	}

	if p.GetStatus() == StatusClosed {
		return fmt.Errorf("%s is closed", p.getInfo())
	}

	if p.GetStatus() == StatusReconnecting {
		logs.LogDebug("[Reliable] %s is reconnecting. The send is blocked", p.getInfo())
		p.reconnectionSignal.L.Lock()
		p.reconnectionSignal.Wait()
		p.reconnectionSignal.L.Unlock()
		logs.LogDebug("[Reliable] %s reconnected. The send is unlocked", p.getInfo())
	}

	return nil
}

func (p *ReliableProducer) checkWriteError(errW error) error {
	if errW != nil {
		switch {
		case errors.Is(errW, stream.FrameTooLarge):
			{
				return stream.FrameTooLarge
			}
		default:
			time.Sleep(500 * time.Millisecond)
			logs.LogError("[Reliable] %s - error during send %s", p.getInfo(), errW.Error())
		}
	}
	return nil
}

func (p *ReliableProducer) Send(message message.StreamMessage) error {
	if err := p.isReadyToSend(); err != nil {
		return err
	}
	p.mutex.Lock()
	errW := p.producer.Send(message)
	p.mutex.Unlock()

	return p.checkWriteError(errW)
}

func (p *ReliableProducer) BatchSend(batchMessages []message.StreamMessage) error {
	if err := p.isReadyToSend(); err != nil {
		return err
	}

	p.mutex.Lock()
	errW := p.producer.BatchSend(batchMessages)
	p.mutex.Unlock()

	return p.checkWriteError(errW)
}

func (p *ReliableProducer) IsOpen() bool {
	p.mutexStatus.Lock()
	defer p.mutexStatus.Unlock()
	return p.status == StatusOpen
}

func (p *ReliableProducer) GetStatus() int {
	p.mutexStatus.Lock()
	defer p.mutexStatus.Unlock()
	return p.status
}

// IReliable interface
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

// End of IReliable interface

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
