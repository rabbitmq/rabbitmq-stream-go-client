package stream

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"sync"
	"sync/atomic"
	"time"
)

var testProducerStream string

var _ = Describe("Streaming Producers", func() {

	BeforeEach(func() {
		time.Sleep(200 * time.Millisecond)
		testProducerStream = uuid.New().String()
		err := testEnvironment.DeclareStream(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())

	})
	AfterEach(func() {
		time.Sleep(200 * time.Millisecond)
		err := testEnvironment.DeleteStream(testProducerStream)
		Expect(err).NotTo(HaveOccurred())

	})

	It("newProducer/Close Publisher", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("newProducer/Publish/Close Publisher", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())

		ids, err1 := producer.BatchPublish(CreateArrayMessagesForTesting(5)) // batch send
		Expect(err1).NotTo(HaveOccurred())
		Expect(len(ids)).To(Equal(5))
		Expect(ids[3]).To(Equal(int64(4)))

		// we can't close the subscribe until the publish is finished
		time.Sleep(500 * time.Millisecond)
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Multi-thread newProducer/Publish", func() {
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				producer, err := testEnvironment.NewProducer(testProducerStream, nil)
				Expect(err).NotTo(HaveOccurred())

				_, err = producer.BatchPublish(CreateArrayMessagesForTesting(5)) // batch send
				Expect(err).NotTo(HaveOccurred())
				// we can't close the subscribe until the publish is finished
				time.Sleep(500 * time.Millisecond)
				err = producer.Close()
				Expect(err).NotTo(HaveOccurred())
			}(&wg)
		}
		wg.Wait()
	})

	It("Not found NotExistingStream", func() {
		_, err := testEnvironment.NewProducer("notExistingStream", nil)
		Expect(err).
			To(Equal(StreamDoesNotExist))
	})

	It("Publish Confirmation", func() {
		var messagesCount int32 = 0

		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			ids := <-ch
			atomic.AddInt32(&messagesCount, int32(len(ids)))
		}(chConfirm)

		_, err = producer.BatchPublish(CreateArrayMessagesForTesting(14))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(14)))

		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Publish handle close", func() {
		var commandIdRecv int32

		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		chConfirm := producer.NotifyClose()
		go func(ch ChannelClose) {
			event := <-ch
			atomic.AddInt32(&commandIdRecv, int32(event.Command))
		}(chConfirm)

		_, err = producer.BatchPublish(CreateArrayMessagesForTesting(14))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
		Expect(atomic.LoadInt32(&commandIdRecv)).To(Equal(int32(CommandDeletePublisher)))
	})

	It("Publish Error", func() {
		var messagesCount int32 = 0

		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		chPublishError := producer.NotifyPublishError()
		go func(ch ChannelPublishError) {
			<-ch
			atomic.AddInt32(&messagesCount, 1)
		}(chPublishError)

		go func(p *Producer) {
			for i := 0; i < 10; i++ {
				_, err := p.BatchPublish(CreateArrayMessagesForTesting(2))
				Expect(err).NotTo(HaveOccurred())
			}
		}(producer)

		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(100 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).NotTo(Equal(int32(0)))
	})

	It("pre Publisher errors / Frame too large / too many messages", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		var arr []message.StreamMessage
		for z := 0; z < 100; z++ {
			s := make([]byte, 15000)
			arr = append(arr, amqp.NewMessage(s))
		}
		_, err = producer.BatchPublish(arr)
		Expect(err).To(Equal(FrameTooLarge))

		for z := 0; z < 901; z++ {
			s := make([]byte, 0)
			arr = append(arr, amqp.NewMessage(s))
		}
		_, err = producer.BatchPublish(arr)
		Expect(err).To(HaveOccurred())
	})

})
