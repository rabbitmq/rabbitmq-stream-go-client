package stream

import (
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

type SuperStreamProducerOptions struct {
	RoutingStrategy RoutingStrategy
}

type SuperStreamProducer struct {
	producers                  []*Producer
	env                        *Environment
	SuperStream                string
	SuperStreamProducerOptions *SuperStreamProducerOptions
}

func newSuperStreamProducer(env *Environment, superStream string, superStreamProducerOptions *SuperStreamProducerOptions) *SuperStreamProducer {
	return &SuperStreamProducer{
		producers:                  make([]*Producer, 0),
		env:                        env,
		SuperStream:                superStream,
		SuperStreamProducerOptions: superStreamProducerOptions,
	}
}

func (s *SuperStreamProducer) init() error {
	partitions, err := s.env.QueryPartitions(s.SuperStream)
	if err != nil {
		return err
	}
	for _, p := range partitions {
		producer, err := s.env.NewProducer(p, nil)
		if err != nil {
			return err
		}
		s.producers = append(s.producers, producer)
	}
	return nil
}

func (s *SuperStreamProducer) GetPartitions() []string {
	partitions := make([]string, 0)
	for _, producer := range s.producers {
		partitions = append(partitions, producer.GetStreamName())
	}
	return partitions
}

func (s *SuperStreamProducer) getProducer(partition string) *Producer {
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
	for _, p := range s.producers {
		err := p.Close()
		if err != nil {
			return err
		}
	}

	s.producers = make([]*Producer, 0)
	return nil
}
