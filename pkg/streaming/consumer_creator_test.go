package streaming

import (
	"fmt"
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"sync/atomic"
	"time"
)

var testConsumerStream string
var _ = Describe("Streaming Consumers", func() {

	BeforeEach(func() {
		testConsumerStream = "testProducerStream"
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
			MessagesHandler(func(consumerId uint8, message *amqp.Message) {
			}).Build()

		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)

		err = consumer.UnSubscribe()
		Expect(err).NotTo(HaveOccurred())

	})

	It("Subscribe Count MessagesHandler", func() {

		producer, err := testClient.ProducerCreator().
			Stream(testConsumerStream).Build()
		Expect(err).NotTo(HaveOccurred())

		_, err = producer.BatchPublish(nil, CreateArrayMessagesForTesting(5)) // batch send
		Expect(err).NotTo(HaveOccurred())
		var count int32

		consumer, err := testClient.ConsumerCreator().
			Stream(testConsumerStream).
			Name("my_consumer").
			MessagesHandler(func(consumerId uint8, message *amqp.Message) {
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

		_, err = producer.BatchPublish(nil, CreateArrayMessagesForTesting(50)) // batch send
		Expect(err).NotTo(HaveOccurred())
		var count int32

		consumer, err := testClient.ConsumerCreator().
			Stream(testConsumerStream).
			Name("my_consumer").
			Offset(OffsetSpecification{}.Offset(30)).
			MessagesHandler(func(consumerId uint8, message *amqp.Message) {
				atomic.AddInt32(&count, 1)

			}).Build()
		time.Sleep(500 * time.Millisecond)
		Expect(err).NotTo(HaveOccurred())
		// just wait a bit until sends the messages
		time.Sleep(200 * time.Millisecond)

		Expect(atomic.LoadInt32(&count)).To(Equal(int32(20)))

		err = consumer.UnSubscribe()
		Expect(err).NotTo(HaveOccurred())
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())

	})

	It("Subscribe stream not found", func() {
		localClient, err := NewClientCreator().Connect()
		Expect(err).NotTo(HaveOccurred())
		_, err = localClient.ConsumerCreator().
			Stream("StreamNotExist").
			Name("my_consumer").
			MessagesHandler(func(consumerId uint8, message *amqp.Message) {

			}).Build()
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Stream does not exist"))
	})
})
