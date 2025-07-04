package ha

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type ReliableSuperStreamConsumer struct {
	env             *stream.Environment
	consumer        atomic.Pointer[stream.SuperStreamConsumer]
	superStreamName string
	consumerOptions *stream.SuperStreamConsumerOptions
	mutexStatus     *sync.Mutex
	messagesHandler stream.MessagesHandler
	status          int
	currentPosition int64 // the last offset consumed. It is needed in case of restart

	//bootstrap: if true the consumer will start from the user offset.
	// If false it will start from the last offset consumed (currentPosition)
	bootstrap bool
}

func NewReliableSuperStreamConsumer(env *stream.Environment, superStream string, messagesHandler stream.MessagesHandler, consumerOptions *stream.SuperStreamConsumerOptions) (*ReliableSuperStreamConsumer, error) {
	if consumerOptions == nil {
		return nil, fmt.Errorf("the consumer options is mandatory")
	}

	if messagesHandler == nil {
		return nil, fmt.Errorf("the messages handler is mandatory")
	}

	res := &ReliableSuperStreamConsumer{
		env:             env,
		superStreamName: superStream,
		consumer:        atomic.Pointer[stream.SuperStreamConsumer]{},
		consumerOptions: consumerOptions,
		mutexStatus:     &sync.Mutex{},
		messagesHandler: messagesHandler,
		status:          StatusClosed,
	}
	consumer, err := env.NewSuperStreamConsumer(superStream, func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		atomic.StoreInt64(&res.currentPosition, consumerContext.Consumer.GetOffset())
		messagesHandler(consumerContext, message)
	}, consumerOptions)
	if err != nil {
		return nil, fmt.Errorf("error creating super stream consumer: %w", err)
	}
	ch := consumer.NotifyPartitionClose(1)
	res.handleNotifyClose(ch)
	res.consumer.Store(consumer)
	res.setStatus(StatusOpen)
	logs.LogDebug("[Reliable] - creating %s", res.getInfo())
	return res, err
}

func (r *ReliableSuperStreamConsumer) handleNotifyClose(channelClose chan stream.CPartitionClose) {
	go func() {
		cPartitionClose := <-channelClose
		if strings.EqualFold(cPartitionClose.Event.Reason, stream.SocketClosed) || strings.EqualFold(cPartitionClose.Event.Reason, stream.MetaDataUpdate) || strings.EqualFold(cPartitionClose.Event.Reason, stream.ZombieConsumer) {
			r.setStatus(StatusReconnecting)
			logs.LogWarn("[Reliable] - %s closed unexpectedly %s.. Reconnecting..", r.getInfo(), cPartitionClose.Event.Reason)
			r.bootstrap = false
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
		}
	}()
}

func (r *ReliableSuperStreamConsumer) setStatus(value int) {
	r.mutexStatus.Lock()
	defer r.mutexStatus.Unlock()
	r.status = value
}

func (r *ReliableSuperStreamConsumer) getInfo() string {
	return fmt.Sprintf("consumer %s for super stream %s",
		r.consumerOptions.ClientProvidedName, r.superStreamName)
}

func (r *ReliableSuperStreamConsumer) getEnv() *stream.Environment {
	return r.env
}

func (r *ReliableSuperStreamConsumer) getNewInstance(partition string) newEntityInstance {
	return func() error {
		return r.consumer.Load().ConnectPartition(partition, stream.OffsetSpecification{}.Offset(r.currentPosition+1))
	}
}

func (r *ReliableSuperStreamConsumer) getTimeOut() time.Duration {
	return time.Duration(3)
}

func (r *ReliableSuperStreamConsumer) GetStreamName() string {
	return r.superStreamName
}

func (r *ReliableSuperStreamConsumer) GetStatus() int {
	r.mutexStatus.Lock()
	defer r.mutexStatus.Unlock()
	return r.status
}

func (r *ReliableSuperStreamConsumer) GetStatusAsString() string {
	return getStatusAsString(r)
}

func (r *ReliableSuperStreamConsumer) Close() error {
	r.setStatus(StatusClosed)
	err := r.consumer.Load().Close()
	if err != nil {
		return err
	}
	return nil

}
