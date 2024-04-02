package stream

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/spaolacci/murmur3"
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
type SuperStreamProducerOptions struct {
	RoutingStrategy               RoutingStrategy
	HandleSuperStreamConfirmation HandleSuperStreamConfirmation
}

type ProducerNotification struct {
	producer         *Producer
	chPublishConfirm ChannelPublishConfirm
	partition        string
}

type SuperStreamPublishConfirm struct {
	Partition          string
	ConfirmationStatus []*ConfirmationStatus
}

type SuperStreamProducer struct {
	producers                  []*ProducerNotification
	env                        *Environment
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
		producers:                  make([]*ProducerNotification, 0),
		env:                        env,
		SuperStream:                superStream,
		SuperStreamProducerOptions: superStreamProducerOptions,
	}, nil
}

func (s *SuperStreamProducer) init() error {
	partitions, err := s.env.QueryPartitions(s.SuperStream)
	if err != nil {
		return err
	}
	for _, p := range partitions {
		producer, err := s.env.NewProducer(p, NewProducerOptions())

		if err != nil {
			return err
		}

		pc := &ProducerNotification{
			producer:         producer,
			chPublishConfirm: producer.NotifyPublishConfirmation(),
			partition:        p,
		}
		s.producers = append(s.producers, pc)

		p := p
		go func() {
			for confirmed := range pc.chPublishConfirm {
				s.SuperStreamProducerOptions.HandleSuperStreamConfirmation(p, &SuperStreamPublishConfirm{
					Partition:          p,
					ConfirmationStatus: confirmed,
				})
			}
		}()
	}
	return nil
}

func (s *SuperStreamProducer) GetPartitions() []string {
	partitions := make([]string, 0)
	for _, producer := range s.producers {
		partitions = append(partitions, producer.partition)
	}
	return partitions
}

func (s *SuperStreamProducer) getProducer(partition string) *Producer {
	for _, p := range s.producers {
		if p.partition == partition {
			return p.producer
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
	for _, p := range s.producers {
		err := p.producer.Close()
		if err != nil {
			return err
		}
	}

	s.producers = make([]*ProducerNotification, 0)
	return nil
}
