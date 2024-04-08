package stream

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"sync"
	"time"
)

type SuperStreamConsumerOptions struct {
	ClientProvidedName   string
	Offset               OffsetSpecification
	SingleActiveConsumer *SingleActiveConsumer
	ConsumerName         string
}

func NewSuperStreamConsumerOptions() *SuperStreamConsumerOptions {
	return &SuperStreamConsumerOptions{
		Offset: OffsetSpecification{}.Next(),
	}
}

func (s *SuperStreamConsumerOptions) SetClientProvidedName(clientProvidedName string) *SuperStreamConsumerOptions {
	s.ClientProvidedName = clientProvidedName
	return s
}

func (s *SuperStreamConsumerOptions) SetOffset(offset OffsetSpecification) *SuperStreamConsumerOptions {
	s.Offset = offset
	return s
}

func (s *SuperStreamConsumerOptions) SetSingleActiveConsumer(singleActiveConsumer *SingleActiveConsumer) *SuperStreamConsumerOptions {
	s.SingleActiveConsumer = singleActiveConsumer
	return s
}

func (s *SuperStreamConsumerOptions) SetConsumerName(consumerName string) *SuperStreamConsumerOptions {
	s.ConsumerName = consumerName
	return s
}

// CPartitionClose is a struct that is used to notify the user when a partition from a consumer is closed
// The user can use the NotifyPartitionClose to get the channel
type CPartitionClose struct {
	Partition string
	Event     Event
	Context   CPartitionContext
}

// CPartitionContext is an interface that is used to expose partition information and methods
// to the user. The user can use the PPartitionContext to reconnect a partition to the SuperStreamConsumer
type CPartitionContext interface {
	ConnectPartition(partition string, offset OffsetSpecification) error
}

type SuperStreamConsumer struct {
	// Only the active consumers are stored here
	activeConsumers []*Consumer
	// we need to copy the partitions here since the
	//activeConsumers is only the consumers active
	// in a normal situation len(partitions) == len(consumers)
	// but in case of disconnection the len(partitions) can be > len(consumers)
	// since the consumer is in reconnection
	partitions                  []string
	env                         *Environment
	mutex                       sync.Mutex
	chSuperStreamPartitionClose chan CPartitionClose

	SuperStream                string
	SuperStreamConsumerOptions *SuperStreamConsumerOptions
	MessagesHandler            MessagesHandler
}

func newSuperStreamConsumer(env *Environment, superStream string, MessagesHandler MessagesHandler, superStreamConsumerOptions *SuperStreamConsumerOptions) (*SuperStreamConsumer, error) {

	if env == nil {
		return nil, ErrEnvironmentNotDefined
	}

	if superStreamConsumerOptions == nil {
		return nil, ErrSuperStreamConsumerOptionsNotDefined
	}

	if superStream == "" || containsOnlySpaces(superStream) {
		return nil, fmt.Errorf("super Stream Name can't be empty")
	}

	logs.LogDebug("Creating a SuperStreamConsumer for: %s", superStream)

	return &SuperStreamConsumer{
		env:                        env,
		SuperStream:                superStream,
		SuperStreamConsumerOptions: superStreamConsumerOptions,
		MessagesHandler:            MessagesHandler,
	}, nil
}

func (s *SuperStreamConsumer) init() error {
	partitions, err := s.env.QueryPartitions(s.SuperStream)
	s.partitions = partitions
	if err != nil {
		return err
	}
	for _, p := range partitions {
		err = s.ConnectPartition(p, s.SuperStreamConsumerOptions.Offset)
		if err != nil {
			return err
		}
	}
	return nil
}

// NotifyPartitionClose returns a channel that will be notified when a partition is closed
// Event will give the reason of the close
// size is the size of the channel
func (s *SuperStreamConsumer) NotifyPartitionClose(size int) chan CPartitionClose {
	ch := make(chan CPartitionClose, size)
	s.chSuperStreamPartitionClose = ch
	return ch
}

func (s *SuperStreamConsumer) getConsumers() []*Consumer {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.activeConsumers
}

func (s *SuperStreamConsumer) ConnectPartition(partition string, offset OffsetSpecification) error {
	logs.LogDebug("[SuperStreamConsumer] ConnectPartition for partition: %s", partition)
	s.mutex.Lock()
	found := false
	for _, p := range s.partitions {
		if p == partition {
			found = true
			break
		}
	}
	if !found {
		s.mutex.Unlock()
		return fmt.Errorf("partition %s not found in the super stream %s", partition, s.SuperStream)
	}
	for _, consumer := range s.activeConsumers {
		if consumer.GetStreamName() == partition {
			s.mutex.Unlock()
			return fmt.Errorf("consumer already connected to: %s partition ", partition)
		}
	}
	s.mutex.Unlock()
	var options = NewConsumerOptions().SetOffset(offset)
	if s.SuperStreamConsumerOptions.ClientProvidedName != "" {
		options = options.SetClientProvidedName(s.SuperStreamConsumerOptions.ClientProvidedName)
	}

	messagesHandler := func(consumerContext ConsumerContext, message *amqp.Message) {
		if s.MessagesHandler != nil {
			s.MessagesHandler(consumerContext, message)
		}
	}
	consumer, err := s.env.NewConsumer(partition, messagesHandler,
		options.SetConsumerName(s.SuperStreamConsumerOptions.ConsumerName).
			SetSingleActiveConsumer(s.SuperStreamConsumerOptions.SingleActiveConsumer))
	if err != nil {
		return err
	}
	s.mutex.Lock()
	s.activeConsumers = append(s.activeConsumers, consumer)
	closedEvent := consumer.NotifyClose()
	s.mutex.Unlock()

	go func(gpartion string, _closedEvent <-chan Event) {
		logs.LogDebug("[SuperStreamConsumer] chSuperStreamPartitionClose started for partition: %s", gpartion)
		event := <-_closedEvent

		s.mutex.Lock()
		for i := range s.activeConsumers {
			if s.activeConsumers[i].GetStreamName() == gpartion {
				s.activeConsumers = append(s.activeConsumers[:i], s.activeConsumers[i+1:]...)
				break
			}
		}
		s.mutex.Unlock()
		if s.chSuperStreamPartitionClose != nil {
			s.mutex.Lock()
			s.chSuperStreamPartitionClose <- CPartitionClose{
				Partition: gpartion,
				Event:     event,
				Context:   s,
			}
			s.mutex.Unlock()
		}
		logs.LogDebug("[SuperStreamConsumer] chSuperStreamPartitionClose for partition: %s", gpartion)
	}(partition, closedEvent)

	return nil
}

func (s *SuperStreamConsumer) Close() error {
	logs.LogDebug("[SuperStreamConsumer] Closing SuperStreamConsumer for: %s", s.SuperStream)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for len(s.activeConsumers) > 0 {
		err := s.activeConsumers[0].Close()
		if err != nil {
			return err
		}
		s.activeConsumers = s.activeConsumers[1:]
	}

	// give the time to raise the close event
	go func() {
		time.Sleep(2 * time.Second)
		s.mutex.Lock()
		if s.chSuperStreamPartitionClose != nil {
			close(s.chSuperStreamPartitionClose)
		}
		s.mutex.Unlock()
	}()

	logs.LogDebug("[SuperStreamConsumer] Closed SuperStreamConsumer for: %s", s.SuperStream)
	return nil
}
