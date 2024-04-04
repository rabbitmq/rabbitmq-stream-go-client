package stream

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/spaolacci/murmur3"
	"sync"
	"time"
)

const SEED = 104729

type RoutingStrategy interface {
	Route(message message.StreamMessage, partitions []string) []string
	SetRouteParameters(superStream string, queryRoute func(superStream string, routingKey string) ([]string, error))
}

// HashRoutingMurmurStrategy is a routing strategy that uses the murmur3 hash function

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
	_, _ = murmurHash.Write([]byte(key))
	murmurHash.Sum32()
	index := murmurHash.Sum32() % uint32(len(partitions))
	return []string{partitions[index]}
}
func (h *HashRoutingMurmurStrategy) SetRouteParameters(_ string, _ func(superStream string, routingKey string) ([]string, error)) {
}

// end of HashRoutingMurmurStrategy

// KeyRoutingStrategy is a routing strategy that uses the key of the message

type KeyRoutingStrategy struct {
	RoutingKeyExtractor func(message message.StreamMessage) string
	UnRoutedMessage     func(message message.StreamMessage, cause error)
	queryRoute          func(superStream string, routingKey string) ([]string, error)
	superStream         string
	cacheRouting        map[string][]string
}

func NewKeyRoutingStrategy(
	routingKeyExtractor func(message message.StreamMessage) string, unRoutedMessage func(message message.StreamMessage, cause error)) *KeyRoutingStrategy {
	return &KeyRoutingStrategy{
		RoutingKeyExtractor: routingKeyExtractor,
		UnRoutedMessage:     unRoutedMessage,
		cacheRouting:        make(map[string][]string),
	}
}

func (k *KeyRoutingStrategy) SetRouteParameters(superStream string, queryRoute func(superStream string, routingKey string) ([]string, error)) {
	k.superStream = superStream
	k.queryRoute = queryRoute
}

func (k *KeyRoutingStrategy) Route(message message.StreamMessage, partitions []string) []string {
	key := k.RoutingKeyExtractor(message)
	var routing []string
	if k.cacheRouting[key] != nil {
		routing = append(routing, k.cacheRouting[key]...)
	} else {
		r, err := k.queryRoute(k.superStream, key)
		routing = append(routing, r...)
		if err != nil {
			k.UnRoutedMessage(message, err)
			return nil
		}
		k.cacheRouting[key] = routing
	}

	for _, p := range partitions {
		for _, r := range routing {
			if r == p {
				return []string{p}
			}
		}
	}
	k.UnRoutedMessage(message, fmt.Errorf("no partition found for key %s", key))
	return nil
}

// end of KeyRoutingStrategy

type SuperStreamProducerOptions struct {
	RoutingStrategy    RoutingStrategy
	ClientProvidedName string
}

// these values are mandatory

func NewSuperStreamProducerOptions(routingStrategy RoutingStrategy) *SuperStreamProducerOptions {
	return &SuperStreamProducerOptions{
		RoutingStrategy: routingStrategy,
	}
}

func (o SuperStreamProducerOptions) SetClientProvidedName(clientProvidedName string) *SuperStreamProducerOptions {
	o.ClientProvidedName = clientProvidedName
	return &o
}

type PartitionPublishConfirm struct {
	Partition          string
	ConfirmationStatus []*ConfirmationStatus
}

type PartitionClose struct {
	Partition string
	Event     Event
	Context   PartitionContext
}

type PartitionContext interface {
	ConnectPartition(partition string) error
}

type SuperStreamProducer struct {
	producers                   []*Producer
	env                         *Environment
	mutex                       sync.Mutex
	SuperStream                 string
	SuperStreamProducerOptions  *SuperStreamProducerOptions
	chNotifyPublishConfirmation chan PartitionPublishConfirm
	chSuperStreamPartitionClose chan PartitionClose
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

	if superStream == "" || containsOnlySpaces(superStream) {

		return nil, fmt.Errorf("super Stream Name can't be empty")
	}

	logs.LogDebug("Creating a SuperStreamProducer for: %s", superStream)
	return &SuperStreamProducer{
		producers:                  make([]*Producer, 0),
		env:                        env,
		SuperStream:                superStream,
		SuperStreamProducerOptions: superStreamProducerOptions,
		mutex:                      sync.Mutex{},
	}, nil
}

func (s *SuperStreamProducer) init() error {
	s.SuperStreamProducerOptions.RoutingStrategy.SetRouteParameters(s.SuperStream, s.env.QueryRoute)
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

	var options = NewProducerOptions()
	if s.SuperStreamProducerOptions.ClientProvidedName != "" {
		options.ClientProvidedName = s.SuperStreamProducerOptions.ClientProvidedName
	}

	producer, err := s.env.NewProducer(partition, options)
	if err != nil {
		return err
	}
	s.mutex.Lock()
	s.producers = append(s.producers, producer)
	chSingleStreamPublishConfirmation := producer.NotifyPublishConfirmation()
	closedEvent := producer.NotifyClose()
	s.mutex.Unlock()

	go func(gpartion string, _closedEvent <-chan Event) {
		logs.LogDebug("[SuperStreamProducer] chSuperStreamPartitionClose started for partition: %s", gpartion)
		event := <-_closedEvent

		s.mutex.Lock()
		for i := range s.producers {
			if s.producers[i].GetStreamName() == gpartion {
				s.producers = append(s.producers[:i], s.producers[i+1:]...)
				break
			}
		}
		s.mutex.Unlock()
		if s.chSuperStreamPartitionClose != nil {
			s.chSuperStreamPartitionClose <- PartitionClose{
				Partition: gpartion,
				Event:     event,
				Context:   s,
			}
		}

		logs.LogDebug("[SuperStreamProducer] chSuperStreamPartitionClose for partition: %s", gpartion)
	}(partition, closedEvent)

	go func(gpartion string, ch <-chan []*ConfirmationStatus) {
		logs.LogDebug("[SuperStreamProducer] chNotifyPublishConfirmation started - partition: %s", gpartion)
		for confirmed := range ch {
			if s.chNotifyPublishConfirmation != nil {
				s.chNotifyPublishConfirmation <- PartitionPublishConfirm{
					Partition:          gpartion,
					ConfirmationStatus: confirmed,
				}
			}
		}
		logs.LogDebug("[SuperStreamProducer] chNotifyPublishConfirmation closed - partition: %s", gpartion)
	}(partition, chSingleStreamPublishConfirmation)

	return nil
}

func (s *SuperStreamProducer) NotifyPublishConfirmation() chan PartitionPublishConfirm {
	ch := make(chan PartitionPublishConfirm, 1)
	s.chNotifyPublishConfirmation = ch
	return ch
}

func (s *SuperStreamProducer) NotifyPartitionClose() chan PartitionClose {
	ch := make(chan PartitionClose, 1)
	s.chSuperStreamPartitionClose = ch
	return ch
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

func (s *SuperStreamProducer) getProducers() []*Producer {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.producers
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
	logs.LogDebug("Closing a SuperStreamProducer for: %s", s.SuperStream)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for len(s.producers) > 0 {
		err := s.producers[0].Close()
		if err != nil {
			return err
		}
		s.producers = s.producers[1:]
	}

	// give the time to raise the close event
	go func() {
		time.Sleep(2 * time.Second)
		if s.chNotifyPublishConfirmation != nil {
			close(s.chNotifyPublishConfirmation)
		}
		if s.chSuperStreamPartitionClose != nil {
			close(s.chSuperStreamPartitionClose)
		}
	}()
	logs.LogDebug("Closed SuperStreamProducer for: %s", s.SuperStream)
	return nil
}
