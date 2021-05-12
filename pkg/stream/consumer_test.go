package stream

import (
	"context"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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
			consumer, err := env.NewConsumer(streamName,
				func(Context ConsumerContext, message *amqp.Message) {

				}, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(consumer.ID).To(Equal(uint8(i % 3)))
			consumers = append(consumers, consumer)
		}

		Expect(len(env.consumers.getCoordinators())).To(Equal(1))
		Expect(len(env.consumers.getCoordinators()["localhost:5551"].
			getClientsPerContext())).To(Equal(4))

		for _, consumer := range consumers {
			err = consumer.UnSubscribe()
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(len(env.consumers.getCoordinators()["localhost:5551"].
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
			consumer, err := env.NewConsumer(streamName,
				func(Context ConsumerContext, message *amqp.Message) {

				}, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(consumer.ID).To(Equal(uint8(i % 2)))
		}

		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(len(env.consumers.getCoordinators()["localhost:5551"].
			getClientsPerContext())).To(Equal(0))

	})

	It("Subscribe and Unsubscribe", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().SetMaxConsumersPerClient(2))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		consumer, err := env.NewConsumer(streamName,
			func(Context ConsumerContext, message *amqp.Message) {

			}, nil)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(10 * time.Millisecond)
		err = consumer.UnSubscribe()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Subscribe/Unsubscribe count messages", func() {
		env, err := NewEnvironment(
			NewEnvironmentOptions().SetMaxConsumersPerClient(2))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		_, err = producer.BatchPublish(context.TODO(), CreateArrayMessagesForTesting(107)) // batch send
		Expect(err).NotTo(HaveOccurred())
		// we can't close the subscribe until the publish is finished
		time.Sleep(500 * time.Millisecond)
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
		var messagesCount int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(Context ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesCount, 1)
				_ = Context.Consumer.Commit()
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(107)))
		err = consumer.UnSubscribe()
		Expect(err).NotTo(HaveOccurred())

		atomic.SwapInt32(&messagesCount, 0)
		consumer, err = env.NewConsumer(streamName,
			func(Context ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesCount, 1)
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.LastConsumed()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(0)))
		err = consumer.UnSubscribe()
		Expect(err).NotTo(HaveOccurred())
		err = env.Close()
		Expect(err).NotTo(HaveOccurred())

	})

	It("Subscribe/Unsubscribe count messages SetOffset", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().SetMaxConsumersPerClient(2))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		_, err = producer.BatchPublish(context.TODO(), CreateArrayMessagesForTesting(100)) // batch send
		Expect(err).NotTo(HaveOccurred())
		// we can't close the subscribe until the publish is finished
		time.Sleep(500 * time.Millisecond)
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
		var messagesCount int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(Context ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesCount, 1)
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.Offset(50)))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(68)))
		err = consumer.UnSubscribe()
		Expect(err).NotTo(HaveOccurred())
	})

	//
	//It("Subscribe Count MessagesHandler SetOffset", func() {
	//
	//	producer, err := testEnvironment.ProducerCreator().
	//		Stream(testConsumerStream).Build()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	for i := 0; i < 5; i++ {
	//		time.Sleep(500 * time.Millisecond)
	//		_, err = producer.BatchPublish(context.TODO(), CreateArrayMessagesForTesting(10)) // batch send
	//		Expect(err).NotTo(HaveOccurred())
	//
	//	}
	//	//var countOffsetTime int32
	//	//consumerOffsetTime, err := testEnvironment.ConsumerOptions().
	//	//	Stream(testConsumerStream).
	//	//	SetConsumerName("my_consumer").
	//	//	SetOffset(Offset{}.Timestamp(time.Now().Add(0*time.Second).Unix() * 1000)).
	//	//	MessagesHandler(func(context ConsumerContext, message *amqp.Message) {
	//	//		atomic.AddInt32(&countOffsetTime, 1)
	//	//
	//	//	}).Build()
	//	//time.Sleep(500 * time.Millisecond)
	//	//// This test is based on time, for the moment I just test the we have some result
	//	//// lower than the full dataset
	//	//Expect(atomic.LoadInt32(&countOffsetTime)).Should(BeNumerically(">", int32(20)))
	//
	//	var countOffset int32
	//	consumerOffSet, err := testEnvironment.ConsumerOptions().
	//		Stream(testConsumerStream).
	//		SetConsumerName("my_consumer").
	//		SetOffset(Offset{}.SetOffset(30)).
	//		MessagesHandler(func(context ConsumerContext, message *amqp.Message) {
	//			atomic.AddInt32(&countOffset, 1)
	//			_ = context.Consumer.Commit()
	//		}).Build()
	//	time.Sleep(300 * time.Millisecond)
	//	Expect(err).NotTo(HaveOccurred())
	//	// just wait a bit until sends the messages
	//	time.Sleep(200 * time.Millisecond)
	//
	//	Expect(atomic.LoadInt32(&countOffset)).To(Equal(int32(20)))
	//
	//	err = consumerOffSet.UnSubscribe()
	//	Expect(err).NotTo(HaveOccurred())
	//	countOffset = 0
	//
	//	consumerLastConsumed, errLast := testEnvironment.ConsumerOptions().
	//		Stream(testConsumerStream).
	//		SetConsumerName("my_consumer").
	//		SetOffset(Offset{}.LastConsumed()).
	//		MessagesHandler(func(context ConsumerContext, message *amqp.Message) {
	//			atomic.AddInt32(&countOffset, 1)
	//			err := context.Consumer.Commit()
	//			Expect(err).NotTo(HaveOccurred())
	//		}).Build()
	//	time.Sleep(300 * time.Millisecond)
	//	Expect(errLast).NotTo(HaveOccurred())
	//	// from last consumed, we don't have other messages to consumer
	//	Expect(atomic.LoadInt32(&countOffset)).To(Equal(int32(0)))
	//
	//	err = consumerLastConsumed.UnSubscribe()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	countOffset = 0
	//	consumerFirst, errFirst := testEnvironment.ConsumerOptions().
	//		Stream(testConsumerStream).
	//		SetConsumerName("my_consumer").
	//		SetOffset(Offset{}.First()).
	//		MessagesHandler(func(context ConsumerContext, message *amqp.Message) {
	//			atomic.AddInt32(&countOffset, 1)
	//			err := context.Consumer.Commit()
	//			Expect(err).NotTo(HaveOccurred())
	//		}).Build()
	//	Expect(errFirst).NotTo(HaveOccurred())
	//	time.Sleep(300 * time.Millisecond)
	//	// from first, we have to read again all the messages
	//	Expect(atomic.LoadInt32(&countOffset)).To(Equal(int32(50)))
	//	time.Sleep(300 * time.Millisecond)
	//	err = consumerFirst.UnSubscribe()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	//err = consumerOffsetTime.UnSubscribe()
	//	//Expect(err).NotTo(HaveOccurred())
	//	err = producer.Close()
	//	Expect(err).NotTo(HaveOccurred())
	//
	//})
	//
	//It("Subscribe stream not found", func() {
	//	consumer, err := testEnvironment.ConsumerOptions().
	//		Stream("StreamNotExist").
	//		SetConsumerName("my_consumer").
	//		MessagesHandler(func(context ConsumerContext, message *amqp.Message) {
	//
	//		}).Build()
	//	Expect(fmt.Sprintf("%s", err)).
	//		To(ContainSubstring("Stream does not exist"))
	//	err = consumer.UnSubscribe()
	//	Expect(fmt.Sprintf("%s", err)).
	//		To(ContainSubstring("Code subscription id does not exist"))
	//
	//})

})
