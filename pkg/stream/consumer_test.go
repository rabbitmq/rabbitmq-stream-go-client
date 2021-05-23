package stream

import (
	"context"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"sync/atomic"
	"time"
)

var _ = Describe("Streaming Consumers", func() {

	BeforeEach(func() {

	})
	AfterEach(func() {

	})

	It("Multi Consumers", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		var consumers []*Consumer

		for i := 0; i < 10; i++ {
			consumer, err := env.NewConsumer(context.TODO(), streamName,
				func(consumerContext ConsumerContext, message *amqp.Message) {

				}, nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(consumer.ID).To(Equal(uint8(i % 3)))
			consumers = append(consumers, consumer)
		}

		Expect(len(env.consumers.getCoordinators())).To(Equal(1))
		Expect(len(env.consumers.getCoordinators()["localhost:5552"].
			getClientsPerContext())).To(Equal(4))

		for _, consumer := range consumers {
			err = consumer.Close()
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(len(env.consumers.getCoordinators()["localhost:5552"].
			getClientsPerContext())).To(Equal(0))

		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Multi Consumers per client", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().SetMaxConsumersPerClient(2))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 10; i++ {
			consumer, err := env.NewConsumer(context.TODO(), streamName,
				func(consumerContext ConsumerContext, message *amqp.Message) {

				}, nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(consumer.ID).To(Equal(uint8(i % 2)))
		}

		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(len(env.consumers.getCoordinators()["localhost:5552"].
			getClientsPerContext())).To(Equal(0))

	})

	It("Subscribe and Unsubscribe", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().
			SetMaxConsumersPerClient(2))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		consumer, err := env.NewConsumer(context.TODO(), streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {

			}, nil, nil)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(10 * time.Millisecond)
		err = consumer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Subscribe fail not exist", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		_, err = env.NewConsumer(context.TODO(), "NOT_EXIST",
			func(consumerContext ConsumerContext, message *amqp.Message) {

			}, nil, nil)

		Expect(errors.Cause(err)).To(Equal(StreamDoesNotExist))
		err = env.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Subscribe/Unsubscribe count messages", func() {
		env, err := NewEnvironment(
			NewEnvironmentOptions().SetMaxConsumersPerClient(2))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		producer, err := env.NewProducer(streamName, nil, nil)
		Expect(err).NotTo(HaveOccurred())

		_, err = producer.BatchPublish(context.TODO(), CreateArrayMessagesForTesting(107)) // batch send
		Expect(err).NotTo(HaveOccurred())
		// we can't close the subscribe until the publish is finished
		time.Sleep(500 * time.Millisecond)
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
		var messagesCount int32 = 0
		consumer, err := env.NewConsumer(context.TODO(), streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesCount, 1)
				_ = consumerContext.Consumer.Commit()
			}, nil, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(107)))
		err = consumer.Close()
		Expect(err).NotTo(HaveOccurred())

		atomic.SwapInt32(&messagesCount, 0)
		consumer, err = env.NewConsumer(context.TODO(), streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesCount, 1)
			}, nil, NewConsumerOptions().SetOffset(OffsetSpecification{}.LastConsumed()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(0)))
		err = consumer.Close()
		Expect(err).NotTo(HaveOccurred())
		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())
		err = env.Close()
		Expect(err).NotTo(HaveOccurred())

	})
	It("Consumer close handler unSubscribe", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		chConsumerClose := make(chan Event)
		var commandIdRecv int32
		go func(ch chan Event) {
			event := <-ch
			atomic.AddInt32(&commandIdRecv, int32(event.Command))
		}(chConsumerClose)

		consumer, err := env.NewConsumer(context.TODO(), streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
			}, chConsumerClose, nil)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(100 * time.Millisecond)
		err = consumer.Close()
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&commandIdRecv)).To(Equal(int32(CommandUnsubscribe)))
		Expect(err).NotTo(HaveOccurred())
		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())
		err = env.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Consumer close handler meta data", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		chConsumerClose := make(chan Event)
		var commandIdRecv int32
		go func(ch chan Event) {
			event := <-ch
			atomic.AddInt32(&commandIdRecv, int32(event.Command))
		}(chConsumerClose)

		_, err = env.NewConsumer(context.TODO(), streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
			}, chConsumerClose, nil)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(100 * time.Millisecond)
		Expect(err).NotTo(HaveOccurred())
		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&commandIdRecv)).To(Equal(int32(CommandMetadataUpdate)))

		err = env.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Subscribe/Unsubscribe count messages SetOffset", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().
			SetMaxConsumersPerClient(2))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		producer, err := env.NewProducer(streamName, nil, nil)
		Expect(err).NotTo(HaveOccurred())

		_, err = producer.BatchPublish(context.TODO(), CreateArrayMessagesForTesting(100)) // batch send
		Expect(err).NotTo(HaveOccurred())
		// we can't close the subscribe until the publish is finished
		time.Sleep(500 * time.Millisecond)
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
		var messagesCount int32 = 0
		consumer, err := env.NewConsumer(context.TODO(), streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesCount, 1)
			}, nil, NewConsumerOptions().SetOffset(OffsetSpecification{}.Offset(50)))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(50)))
		err = consumer.Close()
		Expect(err).NotTo(HaveOccurred())
	})
})
