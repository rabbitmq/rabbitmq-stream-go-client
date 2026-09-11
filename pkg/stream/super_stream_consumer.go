package stream

import (
	"errors"
	"fmt"
	"sync"

	"slices"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
)

type SuperStreamConsumerOptions struct {
	ClientProvidedName   string
	Offset               OffsetSpecification
	Filter               *ConsumerFilter
	SingleActiveConsumer *SingleActiveConsumer
	ConsumerName         string
	AutoCommitStrategy   *AutoCommitStrategy
	Autocommit           bool
}

func NewSuperStreamConsumerOptions() *SuperStreamConsumerOptions {
	return &SuperStreamConsumerOptions{
		Offset:     OffsetSpecification{}.Next(),
		Autocommit: false,
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

func (s *SuperStreamConsumerOptions) SetFilter(filter *ConsumerFilter) *SuperStreamConsumerOptions {
	s.Filter = filter
	return s
}

func (s *SuperStreamConsumerOptions) SetConsumerName(consumerName string) *SuperStreamConsumerOptions {
	s.ConsumerName = consumerName
	return s
}

func (s *SuperStreamConsumerOptions) SetAutoCommit(autoCommitStrategy *AutoCommitStrategy) *SuperStreamConsumerOptions {
	s.Autocommit = true
	s.AutoCommitStrategy = autoCommitStrategy
	return s
}

func (s *SuperStreamConsumerOptions) SetManualCommit() *SuperStreamConsumerOptions {
	s.Autocommit = false
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
// to the user. The user can use the CPartitionContext to reconnect a partition to the SuperStreamConsumer
// Specifying the offset to start from
type CPartitionContext interface {
	ConnectPartition(partition string, offset OffsetSpecification) error
}

type SuperStreamConsumer struct {
	// Only the active consumers are stored here
	activeConsumers []*Consumer
	// we need to copy the partitions here since the
	// activeConsumers is only the consumers active
	// in a normal situation len(partitions) == len(consumers)
	// but in case of disconnection the len(partitions) can be > len(consumers)
	// since the consumer is in reconnection
	partitions []string
	env        *Environment
	mutex      sync.Mutex

	chSuperStreamPartitionMutex  sync.Mutex
	chSuperStreamPartitionClose  chan CPartitionClose
	pendingPartitionClose        []CPartitionClose
	partitionNotificationsClosed bool
	partitionCloseWorkers        sync.WaitGroup
	stopPartitionForwarding      chan struct{}
	closed                       bool

	SuperStream                string
	SuperStreamConsumerOptions *SuperStreamConsumerOptions

	MessagesHandler MessagesHandler
}

func newSuperStreamConsumer(env *Environment, superStream string, messagesHandler MessagesHandler, superStreamConsumerOptions *SuperStreamConsumerOptions) (*SuperStreamConsumer, error) {
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
		stopPartitionForwarding:    make(chan struct{}),
		SuperStream:                superStream,
		SuperStreamConsumerOptions: superStreamConsumerOptions,
		MessagesHandler:            messagesHandler,
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

// NotifyPartitionClose returns the partition close notification channel. Events
// emitted before registration are retained. The buffer holds at least size events
// and is enlarged to hold one event per partition or retained events. Repeated
// calls return the same channel. Close does not wait for readers: if the buffer
// is already full during shutdown, further notifications may be discarded.
func (s *SuperStreamConsumer) NotifyPartitionClose(size int) chan CPartitionClose {
	s.chSuperStreamPartitionMutex.Lock()
	defer s.chSuperStreamPartitionMutex.Unlock()
	if s.chSuperStreamPartitionClose == nil {
		s.chSuperStreamPartitionClose = make(chan CPartitionClose, max(size, len(s.partitions), len(s.pendingPartitionClose)))
		for _, event := range s.pendingPartitionClose {
			s.chSuperStreamPartitionClose <- event
		}
		s.pendingPartitionClose = nil
		if s.partitionNotificationsClosed {
			close(s.chSuperStreamPartitionClose)
		}
	}
	return s.chSuperStreamPartitionClose
}

func (s *SuperStreamConsumer) notifyPartitionClose(event CPartitionClose) {
	s.chSuperStreamPartitionMutex.Lock()
	if s.chSuperStreamPartitionClose == nil {
		s.pendingPartitionClose = append(s.pendingPartitionClose, event)
		s.chSuperStreamPartitionMutex.Unlock()
		return
	}
	ch := s.chSuperStreamPartitionClose
	s.chSuperStreamPartitionMutex.Unlock()
	// Prefer retaining the event whenever buffer space exists, including after
	// Close begins. An abandoned reader must not block shutdown indefinitely.
	select {
	case ch <- event:
		return
	default:
	}
	select {
	case ch <- event:
	case <-s.stopPartitionForwarding:
	}
}

func (s *SuperStreamConsumer) getConsumers() []*Consumer {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.activeConsumers
}

func (s *SuperStreamConsumer) ConnectPartition(partition string, offset OffsetSpecification) error {
	logs.LogDebug("[SuperStreamConsumer] ConnectPartition for partition: %s", partition)
	s.mutex.Lock()
	if s.closed {
		s.mutex.Unlock()
		return AlreadyClosed
	}
	found := slices.Contains(s.partitions, partition)
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

	options = options.SetFilter(s.SuperStreamConsumerOptions.Filter)

	if s.SuperStreamConsumerOptions.Autocommit {
		options = options.SetAutoCommit(s.SuperStreamConsumerOptions.AutoCommitStrategy)
	} else {
		options = options.SetManualCommit()
	}

	if s.SuperStreamConsumerOptions.SingleActiveConsumer != nil {
		// mandatory to enable the super stream consumer
		// we need to create a new SAC for each consumer since we need to store: `offsetSpecification OffsetSpecification`
		// differently. We can leave the ConsumerUpdate pointer to be the same for all the consumers.
		// ConsumerUpdate contains all the info to work alone
		sacForConsumer :=
			newSingleActiveConsumerWithAllParameters(
				s.SuperStreamConsumerOptions.SingleActiveConsumer.ConsumerUpdate,
				s.SuperStreamConsumerOptions.SingleActiveConsumer.Enabled,
				s.SuperStream)
		options = options.SetSingleActiveConsumer(sacForConsumer)
	}

	// set the same handler for all the partitions
	// with consumerContext.Consumer.GetStreamName() it is possible to know the partition
	// and to handle the message in a different way
	// s.MessagesHandler is not mandatory even if it is a good practice to set it
	messagesHandler := func(consumerContext ConsumerContext, message *amqp.Message) {
		if s.MessagesHandler != nil {
			s.MessagesHandler(consumerContext, message)
		} else {
			logs.LogWarn("[SuperStreamConsumer] No handler set for partition: %s", consumerContext.Consumer.GetStreamName())
		}
	}
	consumer, err := s.env.NewConsumer(partition, messagesHandler,
		options.SetConsumerName(s.SuperStreamConsumerOptions.ConsumerName))
	if err != nil {
		return err
	}
	s.mutex.Lock()
	if s.closed {
		s.mutex.Unlock()
		_ = consumer.Close()
		return AlreadyClosed
	}
	s.activeConsumers = append(s.activeConsumers, consumer)
	s.partitionCloseWorkers.Add(1)
	closedEvent := consumer.NotifyClose()
	s.mutex.Unlock()

	go func(gpartion string, _closedEvent <-chan Event) {
		defer s.partitionCloseWorkers.Done()
		logs.LogDebug("[SuperStreamConsumer] chSuperStreamPartitionClose started for partition: %s", gpartion)
		// one shot event
		event := <-_closedEvent
		s.mutex.Lock()
		for i := range s.activeConsumers {
			if s.activeConsumers[i].GetStreamName() == gpartion {
				s.activeConsumers = append(s.activeConsumers[:i], s.activeConsumers[i+1:]...)
				break
			}
		}
		s.mutex.Unlock()
		s.notifyPartitionClose(CPartitionClose{Partition: gpartion, Event: event, Context: s})
		logs.LogDebug("[SuperStreamConsumer] chSuperStreamPartitionClose for partition: %s", gpartion)
	}(partition, closedEvent)

	return nil
}

func (s *SuperStreamConsumer) Close() error {
	logs.LogDebug("[SuperStreamConsumer] Closing SuperStreamConsumer for: %s", s.SuperStream)
	s.mutex.Lock()
	if s.closed {
		s.mutex.Unlock()
		return nil
	}
	s.closed = true
	if s.stopPartitionForwarding != nil {
		close(s.stopPartitionForwarding)
	}
	consumers := s.activeConsumers
	s.activeConsumers = nil
	s.mutex.Unlock()
	var result error
	for _, consumer := range consumers {
		if err := consumer.Close(); err != nil && !errors.Is(err, AlreadyClosed) {
			result = errors.Join(result, err)
		}
	}
	// Each registered partition worker finishes before the notification channel
	// is closed. No timer or caller registration is needed.
	go func() {
		s.partitionCloseWorkers.Wait()
		s.chSuperStreamPartitionMutex.Lock()
		defer s.chSuperStreamPartitionMutex.Unlock()
		s.partitionNotificationsClosed = true
		if s.chSuperStreamPartitionClose != nil {
			close(s.chSuperStreamPartitionClose)
		}
	}()
	logs.LogDebug("[SuperStreamConsumer] Closed SuperStreamConsumer for: %s", s.SuperStream)
	return result
}
