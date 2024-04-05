package stream

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"sync"
)

type SuperStreamConsumerOptions struct {
	ClientProvidedName string
	Offset             OffsetSpecification
}

type SuperStreamConsumer struct {
	// Only the active consumers are stored here
	activeConsumers []*Consumer
	// we need to copy the partitions here since the
	//activeProducers is only the producers active
	// in a normal situation len(partitions) == len(consumers)
	// but in case of disconnection the len(partitions) can be > len(consumers)
	// since the consumer is in reconnection
	partitions []string
	env        *Environment
	mutex      sync.Mutex

	SuperStream                string
	SuperStreamConsumerOptions *SuperStreamConsumerOptions
	MessagesHandler            MessagesHandler
}

func newSuperStreamConsumer(env *Environment, superStream string, MessagesHandler MessagesHandler, options *SuperStreamConsumerOptions) *SuperStreamConsumer {
	return &SuperStreamConsumer{
		env:                        env,
		SuperStream:                superStream,
		SuperStreamConsumerOptions: options,
		MessagesHandler:            MessagesHandler,
	}
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

	options := NewConsumerOptions().SetOffset(offset)
	messagesHandler := func(consumerContext ConsumerContext, message *amqp.Message) {
		s.MessagesHandler(consumerContext, message)
	}
	consumer, err := s.env.NewConsumer(partition, messagesHandler, options)
	if err != nil {
		return err
	}
	s.mutex.Lock()
	s.activeConsumers = append(s.activeConsumers, consumer)
	s.mutex.Unlock()

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

	logs.LogDebug("[SuperStreamConsumer] Closed SuperStreamConsumer for: %s", s.SuperStream)
	return nil
}
