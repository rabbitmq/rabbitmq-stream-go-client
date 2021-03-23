package streaming

import (
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strconv"
	"sync"
	"time"
)

var testClient *Client
var testStreamName string
var _ = BeforeSuite(func() {
	client, err := NewClientCreator().Connect()
	testClient = client
	Expect(err).NotTo(HaveOccurred())
	testStreamName = uuid.New().String()
})

var _ = AfterSuite(func() {
	testClient.Close()
	time.Sleep(500 * time.Millisecond)
	Expect(testClient.producers.Count()).To(Equal(0))
	Expect(testClient.responses.Count()).To(Equal(0))
	Expect(testClient.consumers.Count()).To(Equal(0))
})

var _ = Describe("Streaming testClient", func() {
	BeforeEach(func() {

	})
	AfterEach(func() {
	})

	Describe("Streaming testClient", func() {
		It("Connection Authentication Failure", func() {
			_, err := NewClientCreator().
				Uri("rabbitmq-StreamCreator://wrong_user:wrong_password@localhost:5551/%2f").
				Connect()
			Expect(fmt.Sprintf("%s", err)).
				To(ContainSubstring("Authentication Failure"))
		})
		It("Connection No Endpoint", func() {
			_, err := NewClientCreator().
				Uri("rabbitmq-StreamCreator://g:g@noendpoint:5551/%2f").
				Connect()
			Expect(fmt.Sprintf("%s", err)).
				To(ContainSubstring("no such host"))
		})

		It("Create Stream", func() {
			err := testClient.StreamCreator().Stream(testStreamName).Create()
			Expect(err).NotTo(HaveOccurred())

		})
		It("New/Close Publisher", func() {
			producer, err := testClient.ProducerCreator().Stream(testStreamName).Build()
			Expect(err).NotTo(HaveOccurred())
			err = producer.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("New/Publish/UnSubscribe Publisher", func() {
			producer, err := testClient.ProducerCreator().Stream(testStreamName).Build()
			Expect(err).NotTo(HaveOccurred())
			var arr []*amqp.Message
			for z := 0; z < 10; z++ {
				arr = append(arr, amqp.NewMessage([]byte("test_"+strconv.Itoa(z))))
			}
			_, err = producer.BatchPublish(nil, arr) // batch send
			Expect(err).NotTo(HaveOccurred())
			// we can't close the subscribe until the publish is finished
			time.Sleep(500 * time.Millisecond)
			err = producer.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("Multi-thread New/Publish/UnSubscribe", func() {
			var wg sync.WaitGroup
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func(wg *sync.WaitGroup) {
					defer wg.Done()
					producer, err := testClient.ProducerCreator().Stream(testStreamName).Build()
					Expect(err).NotTo(HaveOccurred())
					var arr []*amqp.Message
					for z := 0; z < 5; z++ {
						arr = append(arr, amqp.NewMessage([]byte("test_"+strconv.Itoa(z))))
					}
					_, err = producer.BatchPublish(nil, arr) // batch send
					Expect(err).NotTo(HaveOccurred())
					// we can't close the subscribe until the publish is finished
					time.Sleep(500 * time.Millisecond)
					err = producer.Close()
					Expect(err).NotTo(HaveOccurred())
				}(&wg)
			}
			wg.Wait()
		})
		It("Delete Stream", func() {
			err := testClient.DeleteStream(testStreamName)
			Expect(err).NotTo(HaveOccurred())
		})
		It("Create two times Stream", func() {
			err := testClient.StreamCreator().Stream(testStreamName).Create()
			Expect(err).NotTo(HaveOccurred())
			err = testClient.StreamCreator().Stream(testStreamName).Create()
			Expect(err).To(HaveOccurred())
			err = testClient.DeleteStream(testStreamName)
			Expect(err).NotTo(HaveOccurred())
		})

		It("Subscribe and Unsubscribe", func() {
			err := testClient.StreamCreator().Stream(testStreamName).Create()
			Expect(err).NotTo(HaveOccurred())

			consumer, err := testClient.ConsumerCreator().
				Stream(testStreamName).
				Name("my_consumer").
				MessagesHandler(func(consumerId uint8, message *amqp.Message) {
				}).Build()

			Expect(err).NotTo(HaveOccurred())
			time.Sleep(500 * time.Millisecond)

			err = consumer.UnSubscribe()
			Expect(err).NotTo(HaveOccurred())
			err = testClient.DeleteStream(testStreamName)
			Expect(err).NotTo(HaveOccurred())

		})

		It("Subscribe Count MessagesHandler", func() {
			err := testClient.StreamCreator().Stream(testStreamName).Create()
			Expect(err).NotTo(HaveOccurred())

			producer, err := testClient.ProducerCreator().Stream(testStreamName).Build()
			Expect(err).NotTo(HaveOccurred())
			var arr []*amqp.Message
			for z := 0; z < 5; z++ {
				arr = append(arr, amqp.NewMessage([]byte("test_"+strconv.Itoa(z))))
			}
			_, err = producer.BatchPublish(nil, arr) // batch send
			Expect(err).NotTo(HaveOccurred())
			var wg sync.WaitGroup
			wg.Add(1)
			count := 0
			consumer, err := testClient.ConsumerCreator().
				Stream(testStreamName).
				Name("my_consumer").
				MessagesHandler(func(consumerId uint8, message *amqp.Message) {
					count++
					if count >= 5 {
						wg.Done()
					}

				}).Build()

			wg.Wait()
			Expect(err).NotTo(HaveOccurred())
			// just wait a bit until sends the messages
			time.Sleep(200 * time.Millisecond)
			Expect(count).To(Equal(5))

			err = consumer.UnSubscribe()
			Expect(err).NotTo(HaveOccurred())
			err = producer.Close()
			Expect(err).NotTo(HaveOccurred())

			err = testClient.DeleteStream(testStreamName)
			Expect(err).NotTo(HaveOccurred())
		})

	})
})
