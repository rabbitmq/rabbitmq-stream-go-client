package streaming

import (
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
	testClient = NewStreamingClient()
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
		It("Connection ", func() {
			err := testClient.Connect("rabbitmq-StreamCreator://guest:guest@localhost:5551/%2f")
			Expect(err).NotTo(HaveOccurred())
		})
		It("Create Stream", func() {
			err := testClient.StreamCreator().Stream(testStreamName).Create()
			Expect(err).NotTo(HaveOccurred())

		})
		It("New/Close Publisher", func() {
			producer, err := testClient.NewProducer(testStreamName)
			Expect(err).NotTo(HaveOccurred())
			err = producer.Close()
			Expect(err).NotTo(HaveOccurred())
		})

		It("New/Publish/UnSubscribe Publisher", func() {
			producer, err := testClient.NewProducer(testStreamName)
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
					producer, err := testClient.NewProducer(testStreamName)
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
			consumer, err := testClient.NewConsumer(testStreamName, func(subscriberId byte, message *amqp.Message) {

			})

			Expect(err).NotTo(HaveOccurred())
			time.Sleep(500 * time.Millisecond)

			err = consumer.UnSubscribe()
			Expect(err).NotTo(HaveOccurred())
			err = testClient.DeleteStream(testStreamName)
			Expect(err).NotTo(HaveOccurred())

		})

		It("Subscribe Count Messages", func() {
			err := testClient.StreamCreator().Stream(testStreamName).Create()
			Expect(err).NotTo(HaveOccurred())

			producer, err := testClient.NewProducer(testStreamName)
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
			consumer, err := testClient.NewConsumer(testStreamName, func(subscriberId byte, message *amqp.Message) {
				count++
				if count >= 5 {
					wg.Done()
				}
			})

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
