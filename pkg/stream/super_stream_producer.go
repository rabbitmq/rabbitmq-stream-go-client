package stream

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/spaolacci/murmur3"
	"sync"
)

const SEED = 104729

type RoutingStrategy interface {
	Route(message message.StreamMessage, partitions []string) []string
}

type HashRoutingMurmurStrategy struct {
	RoutingKeyExtractor func(message message.StreamMessage) string
}

func NewHashRoutingMurmurStrategy(routingKeyExtractor func(message message.StreamMessage) string) *HashRoutingMurmurStrategy {
	return &HashRoutingMurmurStrategy{
		RoutingKeyExtractor: routingKeyExtractor,
	}
}

func (h *HashRoutingMurmurStrategy) Route(message message.StreamMessage, partitions []string) []string {

	key := h.RoutingKeyExtractor(message)
	murmurHash := murmur3.New32WithSeed(SEED)
	murmurHash.Write([]byte(key))
	murmurHash.Sum32()
	index := murmurHash.Sum32() % uint32(len(partitions))
	return []string{partitions[index]}
}

type HandleSuperStreamConfirmation = func(partition string, confirmationStatus *SuperStreamPublishConfirm)
type HandlePartitionClose = func(partition string, event Event)

type SuperStreamProducerOptions struct {
	RoutingStrategy               RoutingStrategy
	HandleSuperStreamConfirmation HandleSuperStreamConfirmation
	HandlePartitionClose          HandlePartitionClose
}

type SuperStreamPublishConfirm struct {
	Partition          string
	ConfirmationStatus []*ConfirmationStatus
}

type SuperStreamProducer struct {
	producers                  []*Producer
	env                        *Environment
	mutex                      sync.Mutex
	SuperStream                string
	SuperStreamProducerOptions *SuperStreamProducerOptions
}

func newSuperStreamProducer(env *Environment, superStream string, superStreamProducerOptions *SuperStreamProducerOptions) (*SuperStreamProducer, error) {

	if env == nil {
		return nil, ErrEnvironmentNotDefined
	}

	if superStreamProducerOptions == nil {
		return nil, ErrSuperStreamProducerOptionsNotDefined
	}

	if superStreamProducerOptions.RoutingStrategy == nil {
		return nil, ErrSuperStreamProducerOptionsNotDefined
	}

	if superStreamProducerOptions.HandleSuperStreamConfirmation == nil {
		return nil, ErrSuperStreamProducerOptionsNotDefined
	}

	if superStream == "" || containsOnlySpaces(superStream) {

		return nil, fmt.Errorf("super Stream Name can't be empty")
	}

	return &SuperStreamProducer{
		producers:                  make([]*Producer, 0),
		env:                        env,
		SuperStream:                superStream,
		SuperStreamProducerOptions: superStreamProducerOptions,
		mutex:                      sync.Mutex{},
	}, nil
}

func (s *SuperStreamProducer) init() error {
	partitions, err := s.env.QueryPartitions(s.SuperStream)
	if err != nil {
		return err
	}
	for _, p := range partitions {
		err = s.ConnectPartition(p)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SuperStreamProducer) ConnectPartition(partition string) error {

	s.mutex.Lock()
	for _, producer := range s.producers {
		if producer.GetStreamName() == partition {
			return fmt.Errorf("partition %s already connected", partition)
		}
	}
	s.mutex.Unlock()

	producer, err := s.env.NewProducer(partition, NewProducerOptions())
	if err != nil {
		return err
	}
	s.mutex.Lock()
	s.producers = append(s.producers, producer)
	chNotifyPublishConfirmation := producer.NotifyPublishConfirmation()
	closedEvent := producer.NotifyClose()
	s.mutex.Unlock()

	go func(gpartion string, _closedEvent <-chan Event) {
		event := <-_closedEvent
		if s.SuperStreamProducerOptions.HandlePartitionClose != nil {
			s.SuperStreamProducerOptions.HandlePartitionClose(gpartion, event)
		}
		s.mutex.Lock()
		for i := range s.producers {
			if s.producers[i].GetStreamName() == gpartion {
				s.producers = append(s.producers[:i], s.producers[i+1:]...)
				break
			}
		}
		s.mutex.Unlock()

	}(partition, closedEvent)

	go func(gpartion string, ch <-chan []*ConfirmationStatus) {
		for confirmed := range ch {
			s.SuperStreamProducerOptions.HandleSuperStreamConfirmation(gpartion, &SuperStreamPublishConfirm{
				Partition:          gpartion,
				ConfirmationStatus: confirmed,
			})
		}
	}(partition, chNotifyPublishConfirmation)

	return nil
}

func (s *SuperStreamProducer) GetPartitions() []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	partitions := make([]string, 0)
	for _, producer := range s.producers {
		partitions = append(partitions, producer.GetStreamName())
	}
	return partitions
}

func (s *SuperStreamProducer) getProducer(partition string) *Producer {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, p := range s.producers {
		if p.GetStreamName() == partition {
			return p
		}
	}
	return nil
}

func (s *SuperStreamProducer) Send(message message.StreamMessage) error {
	b, err := message.MarshalBinary()
	if err != nil {
		return err
	}

	ps := s.SuperStreamProducerOptions.RoutingStrategy.Route(message, s.GetPartitions())
	for _, p := range ps {
		producer := s.getProducer(p)
		if producer == nil {
			return ErrProducerNotFound
		}

		err = producer.sendBytes(message, b)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SuperStreamProducer) Close() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for len(s.producers) > 0 {
		err := s.producers[0].Close()
		if err != nil {
			return err
		}
		s.producers = s.producers[1:]
	}
	return nil
}
