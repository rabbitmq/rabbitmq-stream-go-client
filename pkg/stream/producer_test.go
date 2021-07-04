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

	It("newProducer/Send/Close Publisher", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())

		err1 := producer.BatchPublish(CreateArrayMessagesForTesting(5)) // batch send
		Expect(err1).NotTo(HaveOccurred())

		// we can't close the subscribe until the publish is finished
		time.Sleep(500 * time.Millisecond)
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Multi-thread newProducer/Send", func() {
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				producer, err := testEnvironment.NewProducer(testProducerStream, nil)
				Expect(err).NotTo(HaveOccurred())

				err = producer.BatchPublish(CreateArrayMessagesForTesting(5)) // batch send
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

	It("Send Confirmation", func() {
		var messagesCount int32 = 0

		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			ids := <-ch
			atomic.AddInt32(&messagesCount, int32(len(ids)))
		}(chConfirm)

		err = producer.BatchPublish(CreateArrayMessagesForTesting(14))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(14)))

		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Send handle close", func() {
		var commandIdRecv int32

		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		chConfirm := producer.NotifyClose()
		go func(ch ChannelClose) {
			event := <-ch
			atomic.AddInt32(&commandIdRecv, int32(event.Command))
		}(chConfirm)

		err = producer.BatchPublish(CreateArrayMessagesForTesting(14))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
		Expect(atomic.LoadInt32(&commandIdRecv)).To(Equal(int32(CommandDeletePublisher)))
	})

	//It("Send Error", func() {
	//	var messagesCount int32 = 0
	//	producer, err := testEnvironment.NewProducer(testProducerStream, nil)
	//	Expect(err).NotTo(HaveOccurred())
	//	chPublishError := producer.NotifyPublishError()
	//	go func(ch ChannelPublishError) {
	//		<-ch
	//		atomic.AddInt32(&messagesCount, 1)
	//	}(chPublishError)
	//
	//	go func(p *Producer) {
	//		for i := 0; i < 10; i++ {
	//			err := p.Send(CreateArrayMessagesForTesting(2))
	//			Expect(err).NotTo(HaveOccurred())
	//		}
	//	}(producer)
	//
	//	err = producer.Close()
	//	Expect(err).NotTo(HaveOccurred())
	//	time.Sleep(100 * time.Millisecond)
	//	Expect(atomic.LoadInt32(&messagesCount)).NotTo(Equal(int32(0)))
	//})

	It("pre Publisher errors / Frame too large / too many messages", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		var arr []message.StreamMessage
		for z := 0; z < 100; z++ {
			s := make([]byte, 15000)
			arr = append(arr, amqp.NewMessage(s))
		}
		err = producer.BatchPublish(arr)
		Expect(err).To(Equal(FrameTooLarge))

		for z := 0; z < 901; z++ {
			s := make([]byte, 0)
			arr = append(arr, amqp.NewMessage(s))
		}
		err = producer.BatchPublish(arr)
		Expect(err).To(HaveOccurred())
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Smart Send", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		var messagesCount int32
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			for ids := range ch {
				atomic.AddInt32(&messagesCount, int32(len(ids)))
			}
		}(chConfirm)

		for z := 0; z < 100; z++ {
			s := make([]byte, 50)
			err = producer.Send(amqp.NewMessage(s))
			Expect(err).NotTo(HaveOccurred())
		}
		time.Sleep(400 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(100)))
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Smart Send", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		var messagesCount int32
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			for ids := range ch {
				atomic.AddInt32(&messagesCount, int32(len(ids)))
			}
		}(chConfirm)

		for z := 0; z < 100; z++ {
			s := make([]byte, 50)
			err = producer.Send(amqp.NewMessage(s))
			Expect(err).NotTo(HaveOccurred())
		}
		time.Sleep(400 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(100)))
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Smart Send send after", func() {
		// this test is need to test the send after
		// and the time check
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		var messagesCount int32
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			for ids := range ch {
				atomic.AddInt32(&messagesCount, int32(len(ids)))
			}
		}(chConfirm)

		for z := 0; z < 5; z++ {
			s := make([]byte, 50)
			err = producer.Send(amqp.NewMessage(s))
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(300)
		}

		for z := 0; z < 5; z++ {
			s := make([]byte, 50)
			err = producer.Send(amqp.NewMessage(s))
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(50 * time.Millisecond)
		}

		time.Sleep(400 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(10)))
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

})
