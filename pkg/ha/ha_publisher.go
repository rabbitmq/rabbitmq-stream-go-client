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
		event := <-channelClose
		unexpected := strings.EqualFold(event.Reason, stream.SocketClosed) || strings.EqualFold(event.Reason, stream.MetaDataUpdate)
		// Close and the decision to reconnect share the status lock, so a
		// close event cannot restart a producer after a terminal Close.
		p.mutexStatus.Lock()
		alreadyClosed := p.status == StatusClosed
		if unexpected && !alreadyClosed {
			p.status = StatusReconnecting
		}
		p.mutexStatus.Unlock()
		if unexpected && !alreadyClosed {
			waitTime := randomWaitWithBackoff(1)
			logs.LogWarn("[Reliable] - %s closed unexpectedly.. Reconnecting in %d milliseconds waiting pending messages", p.getInfo(), waitTime)
			var err error
			reconnected := false
			timer := time.NewTimer(time.Duration(waitTime) * time.Millisecond)
			select {
			case <-timer.C:
				err, reconnected = retry(1, p, p.GetStreamName())
			case <-p.stopRetry:
				timer.Stop()
				err = stream.AlreadyClosed
			}
			if err != nil {
				logs.LogInfo(
					"[Reliable] - %s won't be reconnected. Error: %s", p.getInfo(), err)
			}
			if reconnected {
				// Close() may have been called while newProducer() was connecting.
				// Decide under mutexStatus so a terminal Close is never overwritten,
				// and clean up the producer that was just created.
				p.mutexStatus.Lock()
				alreadyClosed = p.status == StatusClosed
				if !alreadyClosed {
					p.status = StatusOpen
				}
				p.mutexStatus.Unlock()

				if alreadyClosed {
					_ = p.producer.Load().Close()
					logs.LogInfo("[Reliable] - %s reconnected but was explicitly closed during reconnection. Closing new producer.", p.getInfo())
				}
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
		logs.LogDebug("[Reliable] - %s reconnection signal sent", p.getInfo())
	}()
}

// ReliableProducer is a producer that can reconnect in case of connection problems
// the function handlePublishConfirm is mandatory
// in case of problems the messages have the message.Confirmed == false
// The functions `Send` and `SendBatch` are blocked during the reconnection
type ReliableProducer struct {
	env                   *stream.Environment
	producer              atomic.Pointer[stream.Producer]
	streamName            string
	producerOptions       *stream.ProducerOptions
	count                 int32
	confirmMessageHandler ConfirmMessageHandler
	mutex                 *sync.Mutex
	mutexStatus           *sync.Mutex
	status                int
	reconnectionSignal    *sync.Cond
	stopRetry             chan struct{}
	retryStopped          bool
}

type ConfirmMessageHandler func(messageConfirm []*stream.ConfirmationStatus)

func NewReliableProducer(env *stream.Environment, streamName string,
	producerOptions *stream.ProducerOptions,
	confirmMessageHandler ConfirmMessageHandler) (*ReliableProducer, error) {
	res := &ReliableProducer{
		env:                   env,
		status:                StatusClosed,
		streamName:            streamName,
		producerOptions:       producerOptions,
		mutex:                 &sync.Mutex{},
		mutexStatus:           &sync.Mutex{},
		confirmMessageHandler: confirmMessageHandler,
		reconnectionSignal:    sync.NewCond(&sync.Mutex{}),
		stopRetry:             make(chan struct{}),
	}
	if confirmMessageHandler == nil {
		return nil, fmt.Errorf("the confirmation message handler is mandatory")
	}
	if producerOptions == nil {
		return nil, fmt.Errorf("the producer options is mandatory")
	}

	// Publish initial state before newProducer starts its close listener.
	res.setStatus(StatusOpen)
	err := res.newProducer()
	if err != nil {
		res.setStatus(StatusClosed)
	}
	return res, err
}

func (p *ReliableProducer) newProducer() error {
	if p.GetStatus() == StatusClosed {
		return stream.AlreadyClosed
	}
	p.mutex.Lock()
	defer p.mutex.Unlock()
	producer, err := p.env.NewProducer(p.streamName, p.producerOptions)
	if err != nil {
		return err
	}
	p.handlePublishConfirm(producer.NotifyPublishConfirmation())
	channelNotifyClose := producer.NotifyClose()
	p.producer.Store(producer)
	p.handleNotifyClose(channelNotifyClose)
	return err
}

func (p *ReliableProducer) Send(message message.StreamMessage) error {
	if err := isReadyToSend(p, p.reconnectionSignal); err != nil {
		return err
	}
	p.mutex.Lock()
	errW := p.producer.Load().Send(message)
	p.mutex.Unlock()

	return checkWriteError(p, errW)
}

func (p *ReliableProducer) BatchSend(batchMessages []message.StreamMessage) error {
	if err := isReadyToSend(p, p.reconnectionSignal); err != nil {
		return err
	}

	p.mutex.Lock()
	errW := p.producer.Load().BatchSend(batchMessages)
	p.mutex.Unlock()

	return checkWriteError(p, errW)
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

func (p *ReliableProducer) GetStatusAsString() string {
	return getStatusAsString(p)
}

// IReliable interface
func (p *ReliableProducer) setStatus(value int) {
	p.mutexStatus.Lock()
	defer p.mutexStatus.Unlock()
	p.status = value
	if value == StatusClosed && p.stopRetry != nil && !p.retryStopped {
		p.retryStopped = true
		close(p.stopRetry)
	}
}

func (p *ReliableProducer) retryStop() <-chan struct{} { return p.stopRetry }

func (p *ReliableProducer) getInfo() string {
	return fmt.Sprintf("producer %s for stream %s",
		p.producerOptions.ClientProvidedName, p.streamName)
}

func (p *ReliableProducer) getEnv() *stream.Environment {
	return p.env
}

func (p *ReliableProducer) getNewInstance(_ string) newEntityInstance {
	return p.newProducer
}

func (p *ReliableProducer) getTimeOut() time.Duration {
	return p.producerOptions.ConfirmationTimeOut
}

func (p *ReliableProducer) GetStreamName() string {
	return p.streamName
}

// End of IReliable interface

func (p *ReliableProducer) GetBroker() *stream.Broker {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return p.producer.Load().GetBroker()
}

func (p *ReliableProducer) Close() error {
	// StatusClosed also stops a pending reconnection.
	p.setStatus(StatusClosed)
	// Load the pointer without the send lock: a blocked send must not delay Close.
	producer := p.producer.Load()
	if producer == nil {
		return nil
	}
	// A reconnecting producer was already closed by the dropped connection,
	// and a repeated Close finds the producer already closed.
	if err := producer.Close(); err != nil && !errors.Is(err, stream.AlreadyClosed) {
		return err
	}
	return nil
}

func (p *ReliableProducer) GetInfo() string {
	return p.getInfo()
}
