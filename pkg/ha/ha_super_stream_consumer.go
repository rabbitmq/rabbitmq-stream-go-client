package ha

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"sync"
	"time"
)

type ReliableSuperStreamConsumer struct {
	env             *stream.Environment
	consumer        *stream.SuperStreamConsumer
	superStreamName string
	consumerOptions *stream.SuperStreamConsumerOptions
	mutexStatus     *sync.Mutex
	status          int
}

func (r ReliableSuperStreamConsumer) setStatus(value int) {
	r.mutexStatus.Lock()
	defer r.mutexStatus.Unlock()
	r.status = value
}

func (r ReliableSuperStreamConsumer) getInfo() string {
	return fmt.Sprintf("consumer %s for super stream %s",
		r.consumerOptions.ClientProvidedName, r.superStreamName)
}

func (r ReliableSuperStreamConsumer) getEnv() *stream.Environment {
	return r.env
}

func (r ReliableSuperStreamConsumer) getNewInstance() newEntityInstance {
	return nil
}

func (r ReliableSuperStreamConsumer) getTimeOut() time.Duration {
	//TODO implement me
	panic("implement me")
}

func (r ReliableSuperStreamConsumer) getStreamName() string {
	return r.superStreamName
}

func (r ReliableSuperStreamConsumer) GetStatus() int {
	r.mutexStatus.Lock()
	defer r.mutexStatus.Unlock()
	return r.status
}

func (r ReliableSuperStreamConsumer) GetStatusAsString() string {
	return getStatusAsString(r)
}
