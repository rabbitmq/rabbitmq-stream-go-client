package stream

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"strconv"
	"sync/atomic"
	"time"
)

var _ = Describe("Streaming Consumers", func() {
	var (
		env        *Environment
		streamName string
	)
	BeforeEach(func() {
		testEnv, err := NewEnvironment(nil)
		env = testEnv
		Expect(err).NotTo(HaveOccurred())
		streamName = uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
	})
	AfterEach(func() {
		err := env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())

	})

	It("Multi Consumers", func() {
		var consumers []*Consumer
		for i := 0; i < 10; i++ {
			consumer, err := env.NewConsumer(streamName,
				func(consumerContext ConsumerContext, message *amqp.Message) {

				}, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(consumer.ID).To(Equal(uint8(0)))
			consumers = append(consumers, consumer)
		}

		Expect(len(env.consumers.getCoordinators())).To(Equal(1))
		Expect(len(env.consumers.getCoordinators()["localhost:5552"].
			getClientsPerContext())).To(Equal(10))

		for _, consumer := range consumers {
			err := consumer.Close()
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(len(env.consumers.getCoordinators()["localhost:5552"].
			getClientsPerContext())).To(Equal(0))

	})

	It("Multi Consumers per client", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().SetMaxConsumersPerClient(2))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 10; i++ {
			consumer, err := env.NewConsumer(streamName,
				func(consumerContext ConsumerContext, message *amqp.Message) {

				}, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(consumer.ID).To(Equal(uint8(i % 2)))
		}

		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())

	})

	It("Subscribe and Unsubscribe", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().
			SetMaxConsumersPerClient(2))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {

			}, nil)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(10 * time.Millisecond)
		err = consumer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Subscribe fail not exist", func() {
		_, err := env.NewConsumer("NOT_EXIST",
			func(consumerContext ConsumerContext, message *amqp.Message) {

			}, nil)

		Expect(errors.Cause(err)).To(Equal(StreamDoesNotExist))
		err = env.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Consumer close handler unSubscribe", func() {
		var commandIdRecv int32

		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
			}, nil)
		Expect(err).NotTo(HaveOccurred())
		chConsumerClose := consumer.NotifyClose()
		go func(ch ChannelClose) {
			event := <-ch
			atomic.AddInt32(&commandIdRecv, int32(event.Command))
		}(chConsumerClose)
		time.Sleep(100 * time.Millisecond)
		err = consumer.Close()
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&commandIdRecv)).To(Equal(int32(CommandUnsubscribe)))
		Expect(err).NotTo(HaveOccurred())
	})

	It("Consumer close handler meta data", func() {
		var commandIdRecv int32
		streamName := uuid.New().String()
		err := env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
			}, nil)
		Expect(err).NotTo(HaveOccurred())
		chConsumerClose := consumer.NotifyClose()
		go func(ch ChannelClose) {
			event := <-ch
			atomic.AddInt32(&commandIdRecv, int32(event.Command))
		}(chConsumerClose)
		time.Sleep(100 * time.Millisecond)
		Expect(err).NotTo(HaveOccurred())
		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&commandIdRecv)).To(Equal(int32(CommandMetadataUpdate)))
		time.Sleep(100 * time.Millisecond)
	})

	It("Subscribe/Unsubscribe count messages SetOffset", func() {
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		err = producer.BatchSend(CreateArrayMessagesForTesting(100)) // batch send
		Expect(err).NotTo(HaveOccurred())
		// we can't close the subscribe until the publish is finished
		time.Sleep(500 * time.Millisecond)
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
		var messagesCount int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesCount, 1)
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.Offset(50)))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(50)))
		err = consumer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Deduplication", func() {
		producer, err := env.NewProducer(streamName, NewProducerOptions().SetProducerName("producer-ded"))
		Expect(err).NotTo(HaveOccurred())
		var arr []message.StreamMessage
		for z := 0; z < 10; z++ {
			m := amqp.NewMessage([]byte("test_" + strconv.Itoa(z)))
			m.SetPublishingId(int64(z * 10))
			arr = append(arr, m)
		}

		// here we handle the deduplication so we must have only
		// 10 messages on the stream, since we are using the
		// same SetPublishingId
		for i := 0; i < 10; i++ {
			err = producer.BatchSend(arr)
		}

		Expect(err).NotTo(HaveOccurred())
		// we can't close the subscribe until the publish is finished
		time.Sleep(500 * time.Millisecond)
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
		var messagesCount int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesCount, 1)
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(10)))
		err = consumer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Subscribe/Unsubscribe count messages", func() {
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		err = producer.BatchSend(CreateArrayMessagesForTesting(107)) // batch send
		Expect(err).NotTo(HaveOccurred())
		// we can't close the subscribe until the publish is finished
		time.Sleep(500 * time.Millisecond)
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
		var messagesCount int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesCount, 1)
				_ = consumerContext.Consumer.StoreOffset()
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(107)))
		err = consumer.Close()
		Expect(err).NotTo(HaveOccurred())

		atomic.SwapInt32(&messagesCount, 0)
		consumer, err = env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesCount, 1)
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.LastConsumed()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(0)))
		err = consumer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Check already closed", func() {
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		err = producer.BatchSend(CreateArrayMessagesForTesting(500)) // batch send
		Expect(err).NotTo(HaveOccurred())
		defer func(producer *Producer) {
			err := producer.Close()
			Expect(err).NotTo(HaveOccurred())
		}(producer)

		var messagesCount int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				if atomic.AddInt32(&messagesCount, 1) >= 1 {
					err := consumerContext.Consumer.Close()
					if err != nil {
						return
					}
				}
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		err = consumer.Close()
		Expect(err).To(Equal(AlreadyClosed))

	})

	It("Validation", func() {
		_, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
			}, &ConsumerOptions{
				Offset: OffsetSpecification{},
			})
		Expect(err).To(HaveOccurred())

	})

})
