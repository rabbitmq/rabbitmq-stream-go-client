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

type SuperStreamProducer struct {
	producers []Producer
}

func NewSuperStreamProducer() *SuperStreamProducer {
	return &SuperStreamProducer{
		producers: make([]Producer, 0),
	}
}

func (s *SuperStreamProducer) AddProducer(producer Producer) {
	s.producers = append(s.producers, producer)
}

func (s *SuperStreamProducer) RemoveProducer(producer Producer) {
	for i, p := range s.producers {
		if p.options.streamName == producer.options.streamName {
			s.producers = append(s.producers[:i], s.producers[i+1:]...)
			break
		}
	}
}
