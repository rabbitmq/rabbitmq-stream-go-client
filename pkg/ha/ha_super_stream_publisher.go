package ha

import (
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
	}

	producer, err := env.NewSuperStreamProducer(superStream, superStreamProducerOptions)
	if err != nil {
		return nil, fmt.Errorf("error creating super stream producer: %w", err)
	}

	ch := producer.NotifyPartitionClose(1)
	res.handleNotifyClose(ch)
	chNotifyPublishConfirm := producer.NotifyPublishConfirmation(1)
	res.handlePublishConfirm(chNotifyPublishConfirm)
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
			if strings.EqualFold(cPartitionClose.Event.Reason, stream.SocketClosed) || strings.EqualFold(cPartitionClose.Event.Reason, stream.MetaDataUpdate) || strings.EqualFold(cPartitionClose.Event.Reason, stream.ZombieConsumer) {
				r.setStatus(StatusReconnecting)
				logs.LogWarn("[Reliable] - %s closed unexpectedly %s.. Reconnecting..", r.getInfo(), cPartitionClose.Event.Reason)
				err, reconnected := retry(1, r, cPartitionClose.Partition)
				if err != nil {
					logs.LogInfo(""+
						"[Reliable] - %s won't be reconnected. Error: %s", r.getInfo(), err)
				}
				if reconnected {
					r.setStatus(StatusOpen)
				} else {
					r.setStatus(StatusClosed)
				}
			} else {
				logs.LogInfo("[Reliable] - %s closed normally. Reason: %s", r.getInfo(), cPartitionClose.Event.Reason)
				r.setStatus(StatusClosed)
				break
			}
			r.reconnectionSignal.L.Lock()
			r.reconnectionSignal.Broadcast()
			r.reconnectionSignal.L.Unlock()
		}
		logs.LogDebug("[ReliableSuperStreamProducer] - closed %s", r.getInfo())
	}()
}

func (r *ReliableSuperStreamProducer) setStatus(value int) {
	r.mutexStatus.Lock()
	defer r.mutexStatus.Unlock()
	r.status = value
}

func (r *ReliableSuperStreamProducer) getInfo() string {
	return fmt.Sprintf("producer %s for super stream %s",
		r.producerOptions.ClientProvidedName, r.superStreamName)
}

func (r *ReliableSuperStreamProducer) getEnv() *stream.Environment {
	return r.env
}

func (r *ReliableSuperStreamProducer) getNewInstance(streamName string) newEntityInstance {
	return func() error {
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
	if err := isReadyToSend(r, r.reconnectionSignal); err != nil {
		return err
	}
	r.mutex.Lock()
	errW := r.producer.Load().Send(message)
	r.mutex.Unlock()

	return checkWriteError(r, errW)
}

func (r *ReliableSuperStreamProducer) Close() error {
	r.setStatus(StatusClosed)
	err := r.producer.Load().Close()
	if err != nil {
		return err
	}
	return nil
}
