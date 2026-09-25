package ha

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type ReliableSuperStreamProducer struct {
	env                            *stream.Environment
	mutex                          *sync.Mutex
	producer                       atomic.Pointer[stream.SuperStreamProducer]
	superStreamName                string
	producerOptions                *stream.SuperStreamProducerOptions
	partitionConfirmMessageHandler PartitionConfirmMessageHandler
	mutexStatus                    *sync.Mutex
	status                         int
	reconnectionSignal             *sync.Cond
	stopRetry                      chan struct{}
	retryStopped                   bool
}

type PartitionConfirmMessageHandler func(messageConfirm []*stream.PartitionPublishConfirm)

func NewReliableSuperStreamProducer(env *stream.Environment, superStream string,
	superStreamProducerOptions *stream.SuperStreamProducerOptions, partitionConfirmMessageHandler PartitionConfirmMessageHandler) (*ReliableSuperStreamProducer, error) {
	if superStreamProducerOptions == nil {
		return nil, fmt.Errorf("the super stream producer options is mandatory")
	}

	if partitionConfirmMessageHandler == nil {
		return nil, fmt.Errorf("the partition confirm message handler is mandatory")
	}

	res := &ReliableSuperStreamProducer{
		env:                env,
		superStreamName:    superStream,
		producerOptions:    superStreamProducerOptions,
		mutexStatus:        &sync.Mutex{},
		mutex:              &sync.Mutex{},
		reconnectionSignal: sync.NewCond(&sync.Mutex{}),
		stopRetry:          make(chan struct{}),
	}

	producer, err := env.NewSuperStreamProducer(superStream, superStreamProducerOptions)
	if err != nil {
		return nil, fmt.Errorf("error creating super stream producer: %w", err)
	}

	ch := producer.NotifyPartitionClose(1)
	res.handlePublishConfirm(producer.NotifyPublishConfirmation(1))
	res.handleNotifyClose(ch)
	res.producer.Store(producer)
	res.partitionConfirmMessageHandler = partitionConfirmMessageHandler
	res.setStatus(StatusOpen)
	return res, nil
}

func (r *ReliableSuperStreamProducer) handlePublishConfirm(confirm chan stream.PartitionPublishConfirm) {
	go func() {
		for c := range confirm {
			r.partitionConfirmMessageHandler([]*stream.PartitionPublishConfirm{&c})
		}
	}()
}

func (r *ReliableSuperStreamProducer) handleNotifyClose(channelClose chan stream.PPartitionClose) {
	go func() {
		for cPartitionClose := range channelClose {
			// Close and the decision to reconnect share the status lock, so a
			// queued event cannot reopen a producer after terminal shutdown.
			r.mutexStatus.Lock()
			if r.status == StatusClosed {
				r.mutexStatus.Unlock()
				r.reconnectionSignal.L.Lock()
				r.reconnectionSignal.Broadcast()
				r.reconnectionSignal.L.Unlock()
				continue
			}
			unexpected := strings.EqualFold(cPartitionClose.Event.Reason, stream.SocketClosed) || strings.EqualFold(cPartitionClose.Event.Reason, stream.MetaDataUpdate) || strings.EqualFold(cPartitionClose.Event.Reason, stream.ZombieConsumer)
			if unexpected {
				r.status = StatusReconnecting
			}
			r.mutexStatus.Unlock()
			if unexpected {
				logs.LogWarn("[Reliable] - %s closed unexpectedly %s.. Reconnecting..", r.getInfo(), cPartitionClose.Event.Reason)
				err, reconnected := retry(1, r, cPartitionClose.Partition)
				if err != nil {
					logs.LogInfo(""+
						"[Reliable] - %s won't be reconnected. Error: %s", r.getInfo(), err)
				}
				if reconnected {
					r.mutexStatus.Lock()
					alreadyClosed := r.status == StatusClosed
					if !alreadyClosed {
						r.status = StatusOpen
					}
					r.mutexStatus.Unlock()
					if alreadyClosed {
						_ = r.producer.Load().Close()
					}
				} else {
					r.setStatus(StatusClosed)
				}
				r.reconnectionSignal.L.Lock()
				r.reconnectionSignal.Broadcast()
				r.reconnectionSignal.L.Unlock()
			} else {
				logs.LogInfo("[Reliable] - %s closed normally. Reason: %s", r.getInfo(), cPartitionClose.Event.Reason)
				r.setStatus(StatusClosed)
				r.reconnectionSignal.L.Lock()
				r.reconnectionSignal.Broadcast()
				r.reconnectionSignal.L.Unlock()
				// Keep draining: the remaining partitions report their close too.
				continue
			}
		}
		logs.LogDebug("[ReliableSuperStreamProducer] - closed %s", r.getInfo())
	}()
}

func (r *ReliableSuperStreamProducer) setStatus(value int) {
	r.mutexStatus.Lock()
	defer r.mutexStatus.Unlock()
	r.status = value
	if value == StatusClosed && r.stopRetry != nil && !r.retryStopped {
		r.retryStopped = true
		close(r.stopRetry)
	}
}

func (r *ReliableSuperStreamProducer) retryStop() <-chan struct{} { return r.stopRetry }

func (r *ReliableSuperStreamProducer) getInfo() string {
	return fmt.Sprintf("producer %s for super stream %s",
		r.producerOptions.ClientProvidedName, r.superStreamName)
}

func (r *ReliableSuperStreamProducer) getEnv() *stream.Environment {
	return r.env
}

func (r *ReliableSuperStreamProducer) getNewInstance(streamName string) newEntityInstance {
	return func() error {
		if r.GetStatus() == StatusClosed {
			return stream.AlreadyClosed
		}
		p := r.producer.Load()
		return p.ConnectPartition(streamName)
	}
}

func (r *ReliableSuperStreamProducer) getTimeOut() time.Duration {
	return time.Duration(3)
}

func (r *ReliableSuperStreamProducer) GetStatus() int {
	r.mutexStatus.Lock()
	defer r.mutexStatus.Unlock()
	return r.status
}

func (r *ReliableSuperStreamProducer) GetStreamName() string {
	return r.superStreamName
}

func (r *ReliableSuperStreamProducer) GetStatusAsString() string {
	return getStatusAsString(r)
}

func (r *ReliableSuperStreamProducer) Send(message message.StreamMessage) error {
	for {
		if err := isReadyToSend(r, r.reconnectionSignal); err != nil {
			return err
		}
		r.mutex.Lock()
		errW := r.producer.Load().Send(message)
		r.mutex.Unlock()

		if errors.Is(errW, stream.ErrProducerNotFound) {
			// The partition producer was already removed from the SuperStreamProducer
			// because its connection just dropped, but this ReliableSuperStreamProducer
			// hasn't processed the close notification yet (status is still StatusOpen).
			// Unlike the single-stream Producer, a super stream partition producer that
			// no longer exists can't record the message as unconfirmed/failed, so it must
			// not be silently dropped here. Wait for the reconnection to be handled and retry.
			r.reconnectionSignal.L.Lock()
			r.reconnectionSignal.Wait()
			r.reconnectionSignal.L.Unlock()
			continue
		}

		return checkWriteError(r, errW)
	}
}

func (r *ReliableSuperStreamProducer) Close() error {
	// StatusClosed also stops a pending reconnection.
	r.setStatus(StatusClosed)
	// Partition producers of a reconnecting super stream producer were already
	// closed by the dropped connection.
	if err := r.producer.Load().Close(); err != nil && !errors.Is(err, stream.AlreadyClosed) {
		return err
	}
	return nil
}
