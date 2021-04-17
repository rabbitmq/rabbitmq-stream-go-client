package streaming

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"sync/atomic"
	"time"
)

var testConsumerStream string
var _ = Describe("Streaming Consumers", func() {

	BeforeEach(func() {
		testConsumerStream = uuid.New().String()
		err := testClient.StreamCreator().Stream(testConsumerStream).Create()
		Expect(err).NotTo(HaveOccurred())

	})
	AfterEach(func() {
		err := testClient.DeleteStream(testConsumerStream)
		Expect(err).NotTo(HaveOccurred())

	})

	It("Subscribe and Unsubscribe", func() {
		consumer, err := testClient.ConsumerCreator().
			Stream(testConsumerStream).
			Name("my_consumer").
			MessagesHandler(func(context ConsumerContext, message *amqp.Message) {

			}).Build()

		Expect(err).NotTo(HaveOccurred())
		err = consumer.UnSubscribe()
		Expect(err).NotTo(HaveOccurred())

	})

	It("Subscribe Count MessagesHandler", func() {

		producer, err := testClient.ProducerCreator().
			Stream(testConsumerStream).Build()
		Expect(err).NotTo(HaveOccurred())

		_, err = producer.BatchPublish(context.TODO(), CreateArrayMessagesForTesting(5)) // batch send
		Expect(err).NotTo(HaveOccurred())
		var count int32

		consumer, err := testClient.ConsumerCreator().
			Stream(testConsumerStream).
			Name("my_consumer").
			MessagesHandler(func(context ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&count, 1)

			}).Build()
		time.Sleep(500 * time.Millisecond)
		Expect(err).NotTo(HaveOccurred())
		// just wait a bit until sends the messages
		time.Sleep(200 * time.Millisecond)

		Expect(atomic.LoadInt32(&count)).To(Equal(int32(5)))

		err = consumer.UnSubscribe()
		Expect(err).NotTo(HaveOccurred())
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())

	})

	It("Subscribe Count MessagesHandler Offset", func() {

		producer, err := testClient.ProducerCreator().
			Stream(testConsumerStream).Build()
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 5; i++ {
			time.Sleep(500 * time.Millisecond)
			_, err = producer.BatchPublish(context.TODO(), CreateArrayMessagesForTesting(10)) // batch send
			Expect(err).NotTo(HaveOccurred())

		}
		//var countOffsetTime int32
		//consumerOffsetTime, err := testClient.ConsumerCreator().
		//	Stream(testConsumerStream).
		//	Name("my_consumer").
		//	Offset(OffsetSpecification{}.Timestamp(time.Now().Add(0*time.Second).Unix() * 1000)).
		//	MessagesHandler(func(context ConsumerContext, message *amqp.Message) {
		//		atomic.AddInt32(&countOffsetTime, 1)
		//
		//	}).Build()
		//time.Sleep(500 * time.Millisecond)
		//// This test is based on time, for the moment I just test the we have some result
		//// lower than the full dataset
		//Expect(atomic.LoadInt32(&countOffsetTime)).Should(BeNumerically(">", int32(20)))

		var countOffset int32
		consumerOffSet, err := testClient.ConsumerCreator().
			Stream(testConsumerStream).
			Name("my_consumer").
			Offset(OffsetSpecification{}.Offset(30)).
			MessagesHandler(func(context ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&countOffset, 1)
				_ = context.Consumer.Commit()
			}).Build()
		time.Sleep(300 * time.Millisecond)
		Expect(err).NotTo(HaveOccurred())
		// just wait a bit until sends the messages
		time.Sleep(200 * time.Millisecond)

		Expect(atomic.LoadInt32(&countOffset)).To(Equal(int32(20)))

		err = consumerOffSet.UnSubscribe()
		Expect(err).NotTo(HaveOccurred())
		countOffset = 0

		consumerLastConsumed, errLast := testClient.ConsumerCreator().
			Stream(testConsumerStream).
			Name("my_consumer").
			Offset(OffsetSpecification{}.LastConsumed()).
			MessagesHandler(func(context ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&countOffset, 1)
				err := context.Consumer.Commit()
				Expect(err).NotTo(HaveOccurred())
			}).Build()
		time.Sleep(300 * time.Millisecond)
		Expect(errLast).NotTo(HaveOccurred())
		// from last consumed, we don't have other messages to consumer
		Expect(atomic.LoadInt32(&countOffset)).To(Equal(int32(0)))

		err = consumerLastConsumed.UnSubscribe()
		Expect(err).NotTo(HaveOccurred())

		countOffset = 0
		consumerFirst, errFirst := testClient.ConsumerCreator().
			Stream(testConsumerStream).
			Name("my_consumer").
			Offset(OffsetSpecification{}.First()).
			MessagesHandler(func(context ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&countOffset, 1)
				err := context.Consumer.Commit()
				Expect(err).NotTo(HaveOccurred())
			}).Build()
		Expect(errFirst).NotTo(HaveOccurred())
		time.Sleep(300 * time.Millisecond)
		// from first, we have to read again all the messages
		Expect(atomic.LoadInt32(&countOffset)).To(Equal(int32(50)))
		time.Sleep(300 * time.Millisecond)
		err = consumerFirst.UnSubscribe()
		Expect(err).NotTo(HaveOccurred())

		//err = consumerOffsetTime.UnSubscribe()
		//Expect(err).NotTo(HaveOccurred())
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())

	})

	It("Subscribe stream not found", func() {
		consumer, err := testClient.ConsumerCreator().
			Stream("StreamNotExist").
			Name("my_consumer").
			MessagesHandler(func(context ConsumerContext, message *amqp.Message) {

			}).Build()
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Stream does not exist"))
		err = consumer.UnSubscribe()
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Code subscription id does not exist"))

	})

})
