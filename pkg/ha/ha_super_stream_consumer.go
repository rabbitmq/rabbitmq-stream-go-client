package ha

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type ReliableSuperStreamConsumer struct {
	env               *stream.Environment
	consumer          atomic.Pointer[stream.SuperStreamConsumer]
	superStreamName   string
	consumerOptions   *stream.SuperStreamConsumerOptions
	mutexStatus       *sync.Mutex
	messagesHandler   stream.MessagesHandler
	status            int
	streamPositionMap sync.Map

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
		res.streamPositionMap.Store(consumerContext.Consumer.GetStreamName(), consumerContext.Consumer.GetOffset())
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
		// for channelClose until closed
		for cPartitionClose := range channelClose {
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
				break
			}
		}
		logs.LogDebug("[ReliableSuperStreamConsumer] - cPartitionClose closed %s", r.getInfo())
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
		c := r.consumer.Load()
		// by default the consumer will start from the consumerOptions.Offset
		off := r.consumerOptions.Offset
		var restartOffset int64
		// in case of there is an item for the partition in the streamPositionMap
		// it will start from the last offset consumed
		v, _ := r.streamPositionMap.Load(partition)
		if v != nil {
			restartOffset = v.(int64)
			off = stream.OffsetSpecification{}.Offset(restartOffset + 1)
		}

		return c.ConnectPartition(partition, off)
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
