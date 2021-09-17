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

var _ = Describe("Stream Producers", func() {

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

	It("NewProducer/Close Publisher", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("NewProducer/Send/Close Publisher", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())

		err1 := producer.BatchSend(CreateArrayMessagesForTesting(5)) // batch send
		Expect(err1).NotTo(HaveOccurred())

		time.Sleep(100 * time.Millisecond)
		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
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

				err = producer.BatchSend(CreateArrayMessagesForTesting(5)) // batch send
				Expect(err).NotTo(HaveOccurred())
				// we can't close to subscribe until the publishing is finished
				time.Sleep(500 * time.Millisecond)
				Expect(len(producer.unConfirmedMessages)).To(Equal(0))

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

		err = producer.BatchSend(CreateArrayMessagesForTesting(14))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(14)))
		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Handle close", func() {
		var commandIdRecv int32

		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		chConfirm := producer.NotifyClose()
		go func(ch ChannelClose) {
			event := <-ch
			atomic.StoreInt32(&commandIdRecv, int32(event.Command))
		}(chConfirm)

		err = producer.BatchSend(CreateArrayMessagesForTesting(2))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(100 * time.Millisecond)
		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(100 * time.Millisecond)
		Expect(atomic.LoadInt32(&commandIdRecv)).To(Equal(int32(CommandDeletePublisher)))
	})

	It("Pre Publisher errors / Frame too large / too many messages", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		var arr []message.StreamMessage
		for z := 0; z < 100; z++ {
			s := make([]byte, 15000)
			arr = append(arr, amqp.NewMessage(s))
		}
		err = producer.BatchSend(arr)
		Expect(err).To(Equal(FrameTooLarge))

		for z := 0; z < 901; z++ {
			s := make([]byte, 0)
			arr = append(arr, amqp.NewMessage(s))
		}
		err = producer.BatchSend(arr)
		Expect(err).To(HaveOccurred())
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Smart Send/Close", func() {
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
		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
		// in this case must raise an error since the producer is closed
		err = producer.Close()
		Expect(err).To(HaveOccurred())
	})

	It("Smart Send Split frame/BatchSize", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchSize(50))
		Expect(err).NotTo(HaveOccurred())
		var messagesCount int32
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			for ids := range ch {
				atomic.AddInt32(&messagesCount, int32(len(ids)))
			}
		}(chConfirm)

		for z := 0; z < 2; z++ {
			s := make([]byte, 1048000)
			err = producer.Send(amqp.NewMessage(s))
			Expect(err).NotTo(HaveOccurred())
		}
		time.Sleep(800 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(2)))
		By("Max frame Error")
		s := make([]byte, 1148576)
		err = producer.Send(amqp.NewMessage(s))
		Expect(err).To(HaveOccurred())
		time.Sleep(100 * time.Millisecond)
		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())

		producer, err = testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchSize(2))
		Expect(err).NotTo(HaveOccurred())
		var messagesCountBatch int32
		chConfirmBatch := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			for ids := range ch {
				atomic.AddInt32(&messagesCountBatch, int32(len(ids)))
			}
		}(chConfirmBatch)

		for i := 0; i < 100; i++ {
			s := make([]byte, 11)
			err = producer.Send(amqp.NewMessage(s))
			Expect(err).NotTo(HaveOccurred())
		}

		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCountBatch)).To(Equal(int32(100)))
		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())

	})

	It("Smart Send send after", func() {
		// this test is need to test "send after"
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
		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Smart Send send after BatchPublishingDelay", func() {
		// this test is need to test "send after BatchPublishingDelay"
		// and the time check
		producer, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchPublishingDelay(50))
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
			time.Sleep(60 * time.Millisecond)
		}

		for z := 0; z < 5; z++ {
			s := make([]byte, 50)
			err = producer.Send(amqp.NewMessage(s))
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(20 * time.Millisecond)
		}

		time.Sleep(400 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).To(Equal(int32(10)))
		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("Already Closed/Limits", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().SetMaxProducersPerClient(5))
		Expect(err).NotTo(HaveOccurred())
		producer, err := env.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		err = producer.Close()
		Expect(err).NotTo(HaveOccurred())

		err = producer.Close()
		Expect(err).To(Equal(AlreadyClosed))

		/// validation limits
		/// options.QueueSize, options.BatchSize and options.BatchPublishingDelay
		_, err = env.NewProducer(testProducerStream, &ProducerOptions{
			QueueSize: 1,
		})
		Expect(err).To(HaveOccurred())

		_, err = env.NewProducer(testProducerStream, NewProducerOptions().
			SetQueueSize(5000000))
		Expect(err).To(HaveOccurred())

		_, err = env.NewProducer(testProducerStream, &ProducerOptions{
			BatchSize: 0,
		})
		Expect(err).To(HaveOccurred())

		_, err = env.NewProducer(testProducerStream, &ProducerOptions{
			BatchSize: 20_000,
		})
		Expect(err).To(HaveOccurred())

		_, err = env.NewProducer(testProducerStream, &ProducerOptions{
			BatchSize: 5_000_000,
		})
		Expect(err).To(HaveOccurred())

		_, err = env.NewProducer(testProducerStream, &ProducerOptions{
			BatchPublishingDelay: 0,
		})
		Expect(err).To(HaveOccurred())

		_, err = env.NewProducer(testProducerStream, &ProducerOptions{
			BatchPublishingDelay: 600,
		})
		Expect(err).To(HaveOccurred())

		_, err = env.NewProducer(testProducerStream,
			NewProducerOptions().SetSubEntrySize(0))

		Expect(err).To(HaveOccurred())

		producerBatch, err := env.NewProducer(testProducerStream,
			NewProducerOptions().SetSubEntrySize(55))
		Expect(err).NotTo(HaveOccurred())
		err = producerBatch.BatchSend(CreateArrayMessagesForTesting(5))
		Expect(err).To(HaveOccurred())

		err = env.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	// this test is needed to test publish error.
	// In order to simulate the producer id not found I need to
	// change manually the producer id.
	// It works, but would be better to introduce some mock function
	It("Publish  Error", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		prodErrorStream := uuid.New().String()
		err = env.DeclareStream(prodErrorStream, nil)
		Expect(err).NotTo(HaveOccurred())

		var messagesCount int32 = 0
		producer, err := testEnvironment.NewProducer(prodErrorStream, nil)
		Expect(err).NotTo(HaveOccurred())
		chPublishError := producer.NotifyPublishError()
		go func(ch ChannelPublishError) {
			<-ch
			atomic.AddInt32(&messagesCount, 1)
		}(chPublishError)

		var messagesSequence = make([]*messageSequence, 1)
		msg := amqp.NewMessage([]byte("test"))
		messageBytes, _ := msg.MarshalBinary()
		messagesSequence[0] = &messageSequence{
			message:      msg,
			size:         len(messageBytes),
			publishingId: 1,
		}
		for _, producerC := range producer.options.client.coordinator.producers {
			producerC.(*Producer).id = uint8(200)
		}
		producer.options.client.coordinator.producers[uint8(200)] = producer
		// 200 producer ID doesn't exist
		subEntryMessage := []subEntryMessage{{messages: messagesSequence}}
		err = producer.internalBatchSendProdId(subEntryMessage, 200)
		Expect(err).NotTo(HaveOccurred())

		err = env.DeleteStream(prodErrorStream)
		Expect(err).NotTo(HaveOccurred())
		delete(producer.options.client.coordinator.producers, uint8(200))
		delete(producer.options.client.coordinator.producers, uint8(0))
		err = env.Close()
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesCount)).NotTo(Equal(int32(0)))
	})

})
