package stream

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/spaolacci/murmur3"
	"sync"
	"time"
)

// The base interface for routing strategies
// The client is shipped with two routing strategies:
// - HashRoutingStrategy
// - KeyRoutingStrategy
// The user can implement its own routing strategy by implementing this interface.
// In most of the cases these two strategies are enough.
// See the test: Implement custom routing strategy in case you need to implement a custom routing strategy

type RoutingStrategy interface {
	//Route Based on the message and the partitions the routing strategy returns the partitions where the message should be sent
	// It could be zero, one or more partitions
	Route(message message.StreamMessage, partitions []string) []string

	// SetRouteParameters is useful for the routing key strategies to set the query route function
	// or in general to set the parameters needed by the routing strategy
	SetRouteParameters(superStream string, queryRoute func(superStream string, routingKey string) ([]string, error))
}

// HashRoutingMurmurStrategy is a routing strategy that uses the murmur3 hash function

// DON'T Touch this value. It makes the hash compatible with the Java/.Net/Python client

const SEED = 104729

type HashRoutingStrategy struct {
	RoutingKeyExtractor func(message message.StreamMessage) string
}

func NewHashRoutingStrategy(routingKeyExtractor func(message message.StreamMessage) string) *HashRoutingStrategy {
	return &HashRoutingStrategy{
		RoutingKeyExtractor: routingKeyExtractor,
	}
}

func (h *HashRoutingStrategy) Route(message message.StreamMessage, partitions []string) []string {

	key := h.RoutingKeyExtractor(message)
	murmurHash := murmur3.New32WithSeed(SEED)
	_, _ = murmurHash.Write([]byte(key))
	murmurHash.Sum32()
	index := murmurHash.Sum32() % uint32(len(partitions))
	return []string{partitions[index]}
}
func (h *HashRoutingStrategy) SetRouteParameters(_ string, _ func(superStream string, routingKey string) ([]string, error)) {
}

// end of HashRoutingMurmurStrategy

// KeyRoutingStrategy is a routing strategy that uses the key of the message
type KeyRoutingStrategy struct {
	// provided by the user to define the key based on a message
	RoutingKeyExtractor func(message message.StreamMessage) string

	// event called when the message is not routed. The user should implement this event
	// to keep track of the messages that are not routed
	UnRoutedMessage func(message message.StreamMessage, cause error)
	queryRoute      func(superStream string, routingKey string) ([]string, error)
	superStream     string
	cacheRouting    map[string][]string
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
	// check if the routing is already in cache.
	// Cache is useful to avoid multiple queries for the same key
	// so only the first message with a key will be queried
	if k.cacheRouting[key] != nil {
		routing = append(routing, k.cacheRouting[key]...)
	} else {
		r, err := k.queryRoute(k.superStream, key)
		if err != nil {
			// The message is not routed due of an error in the queryRoute
			k.UnRoutedMessage(message, err)
			return nil
		}
		routing = append(routing, r...)
		k.cacheRouting[key] = routing
	}

	for _, p := range partitions {
		for _, r := range routing {
			if r == p {
				return []string{p}
			}
		}
	}

	// The message is not routed since does not have a partition based on the key
	// It can happen if the key selected is not in the routing table
	// differently from the hash strategy the key strategy can have zero partitions
	k.UnRoutedMessage(message, fmt.Errorf("no partition found for key %s", key))
	return nil
}

// end of KeyRoutingStrategy

type SuperStreamProducerOptions struct {
	RoutingStrategy    RoutingStrategy
	ClientProvidedName string
}

// NewSuperStreamProducerOptions creates a new SuperStreamProducerOptions
// The RoutingStrategy is mandatory
func NewSuperStreamProducerOptions(routingStrategy RoutingStrategy) *SuperStreamProducerOptions {
	return &SuperStreamProducerOptions{
		RoutingStrategy: routingStrategy,
	}
}

func (o SuperStreamProducerOptions) SetClientProvidedName(clientProvidedName string) *SuperStreamProducerOptions {
	o.ClientProvidedName = clientProvidedName
	return &o
}

// PartitionPublishConfirm is a struct that is used to notify the user when a message is confirmed or not per partition
// The user can use the NotifyPublishConfirmation to get the channel
type PartitionPublishConfirm struct {
	Partition          string
	ConfirmationStatus []*ConfirmationStatus
}

// PartitionClose is a struct that is used to notify the user when a partition is closed
// The user can use the NotifyPartitionClose to get the channel
type PartitionClose struct {
	Partition string
	Event     Event
	Context   PartitionContext
}

// PartitionContext is an interface that is used to expose partition information and methods
// to the user. The user can use the PartitionContext to reconnect a partition to the SuperStreamProducer
type PartitionContext interface {
	ConnectPartition(partition string) error
}

type SuperStreamProducer struct {
	// Only the active producers are stored here
	activeProducers []*Producer
	// we need to copy the partitions here since the
	//activeProducers is only the producers active
	// in a normal situation len(partitions) == len(activeProducers)
	// but in case of disconnection the len(partitions) can be > len(activeProducers)
	// since the producer is in reconnection
	partitions []string

	env                         *Environment
	mutex                       sync.Mutex
	chNotifyPublishConfirmation chan PartitionPublishConfirm
	chSuperStreamPartitionClose chan PartitionClose

	// public
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

	if superStream == "" || containsOnlySpaces(superStream) {

		return nil, fmt.Errorf("super Stream Name can't be empty")
	}

	logs.LogDebug("Creating a SuperStreamProducer for: %s", superStream)
	return &SuperStreamProducer{
		activeProducers:            make([]*Producer, 0),
		env:                        env,
		SuperStream:                superStream,
		SuperStreamProducerOptions: superStreamProducerOptions,
		mutex:                      sync.Mutex{},
	}, nil
}

func (s *SuperStreamProducer) init() error {
	// set the routing strategy parameters
	s.SuperStreamProducerOptions.RoutingStrategy.SetRouteParameters(s.SuperStream, s.env.QueryRoute)

	partitions, err := s.env.QueryPartitions(s.SuperStream)
	s.partitions = partitions
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

// ConnectPartition connects a partition to the SuperStreamProducer part of PartitionContext interface
// The super stream producer is a producer that can send messages to multiple partitions
// that are hidden to the user.
// with the ConnectPartition the user can re-connect a partition to the SuperStreamProducer
// that should be used only in case of disconnection
func (s *SuperStreamProducer) ConnectPartition(partition string) error {
	logs.LogDebug("[SuperStreamProducer] ConnectPartition for partition: %s", partition)
	s.mutex.Lock()
	for _, producer := range s.activeProducers {
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
	s.activeProducers = append(s.activeProducers, producer)
	chSingleStreamPublishConfirmation := producer.NotifyPublishConfirmation()
	closedEvent := producer.NotifyClose()
	s.mutex.Unlock()

	go func(gpartion string, _closedEvent <-chan Event) {
		logs.LogDebug("[SuperStreamProducer] chSuperStreamPartitionClose started for partition: %s", gpartion)
		event := <-_closedEvent

		s.mutex.Lock()
		for i := range s.activeProducers {
			if s.activeProducers[i].GetStreamName() == gpartion {
				s.activeProducers = append(s.activeProducers[:i], s.activeProducers[i+1:]...)
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

// NotifyPublishConfirmation returns a channel that will be notified when a message is confirmed or not per partition
func (s *SuperStreamProducer) NotifyPublishConfirmation() chan PartitionPublishConfirm {
	ch := make(chan PartitionPublishConfirm, 1)
	s.chNotifyPublishConfirmation = ch
	return ch
}

// NotifyPartitionClose returns a channel that will be notified when a partition is closed
// Event will give the reason of the close
func (s *SuperStreamProducer) NotifyPartitionClose() chan PartitionClose {
	ch := make(chan PartitionClose, 1)
	s.chSuperStreamPartitionClose = ch
	return ch
}

func (s *SuperStreamProducer) GetPartitions() []string {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.partitions
}

func (s *SuperStreamProducer) getProducer(partition string) *Producer {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, p := range s.activeProducers {
		if p.GetStreamName() == partition {
			return p
		}
	}
	return nil
}

func (s *SuperStreamProducer) getProducers() []*Producer {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.activeProducers
}

// Send sends a message to the partitions based on the routing strategy
func (s *SuperStreamProducer) Send(message message.StreamMessage) error {
	b, err := message.MarshalBinary()
	if err != nil {
		return err
	}

	ps := s.SuperStreamProducerOptions.RoutingStrategy.Route(message, s.GetPartitions())
	for _, p := range ps {
		producer := s.getProducer(p)
		if producer == nil {
			// the producer is not. It can happen if the tcp connection for the partition is dropped
			// the user can reconnect the partition using the ConnectPartition
			// The client returns an error. Even there could be other partitions where the message can be sent.
			// but won't to that to break the expectation of the user. The routing should be always the same
			// for the same message. The user has to handle the error and decide to send the message again
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
	for len(s.activeProducers) > 0 {
		err := s.activeProducers[0].Close()
		if err != nil {
			return err
		}
		s.activeProducers = s.activeProducers[1:]
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
