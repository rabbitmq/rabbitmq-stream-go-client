package stream

import (
	"fmt"
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
		Expect(testEnvironment.DeclareStream(testProducerStream, nil)).
			NotTo(HaveOccurred())

	})
	AfterEach(func() {
		time.Sleep(200 * time.Millisecond)
		Expect(testEnvironment.DeleteStream(testProducerStream)).NotTo(HaveOccurred())

	})

	It("NewProducer/Close Publisher", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("NewProducer/Send/Close Publisher", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(producer.BatchSend(CreateArrayMessagesForTesting(5))).NotTo(HaveOccurred())
		// we can't close the subscribe until the publish is finished
		time.Sleep(200 * time.Millisecond)
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("Multi-thread newProducer/Send", func() {
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer wg.Done()
				producer, err := testEnvironment.NewProducer(testProducerStream, nil)
				Expect(err).NotTo(HaveOccurred())

				Expect(producer.BatchSend(CreateArrayMessagesForTesting(5))).NotTo(HaveOccurred())
				// we can't close the subscribe until the publish is finished
				time.Sleep(200 * time.Millisecond)
				err = producer.Close()
				Expect(err).NotTo(HaveOccurred())
			}(&wg)
		}
		wg.Wait()
	})

	Context("Sending messages via a single stream producer concurrently (from many threads)", func() {
		var (
			producer         *Producer
			wg               sync.WaitGroup
			messagesReceived int32 = 0
		)
		const (
			ThreadCount                = 3
			BatchSize                  = 100
			SubEntrySize               = 100
			TotalMessageCountPerThread = 1000
		)

		When("No batching, nor sub-entry. nor compression", func() {

			BeforeEach(func() {
				producer = createProducer(
					// batching (100), no sub-entry and hence no compression
					NewProducerOptions().SetBatchSize(1).SetSubEntrySize(1),
					&messagesReceived)

				wg.Add(ThreadCount)
			})

			AfterEach(func() {
				err := producer.Close()
				Expect(err).NotTo(HaveOccurred())
			})

			FIt("Send plain messages synchronously", func() {
				sendSynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread, 1)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)

			})
			It("Send plain messages asynchronously", func() {
				sendAsynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)
			})

		})
		When("Batching enabled, but without sub-entry and compression", func() {

			BeforeEach(func() {
				producer = createProducer(
					// batching (100), no sub-entry and hence no compression
					NewProducerOptions().SetBatchSize(BatchSize).SetSubEntrySize(1),
					&messagesReceived)

				wg.Add(ThreadCount)
			})

			AfterEach(func() {
				err := producer.Close()
				Expect(err).NotTo(HaveOccurred())
			})

			It("Send batched messages synchronously", func() {
				sendSynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread, BatchSize)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)
			})

			It("Send batched messages asynchronously", func() {
				sendAsynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)
			})
		})

		When("Batching and sub-entry enabled without compression", func() {

			BeforeEach(func() {
				producer = createProducer(
					// batching (100), sub-entry (100) and no compression
					NewProducerOptions().SetBatchSize(BatchSize).SetSubEntrySize(SubEntrySize).SetCompression(Compression{}.None()),
					&messagesReceived)

				wg.Add(ThreadCount)
			})

			AfterEach(func() {
				err := producer.Close()
				Expect(err).NotTo(HaveOccurred())
			})

			It("Send batched and subentry messages synchronously", func() {
				sendSynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread, BatchSize)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)
			})

			It("Send batched messages asynchronously", func() {
				sendAsynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)
			})
		})

		When("Batching and sub-entry enabled with GZIP compression", func() {

			BeforeEach(func() {
				producer = createProducer(
					// batching (100), sub-entry (100) and no compression
					NewProducerOptions().SetBatchSize(BatchSize).SetSubEntrySize(SubEntrySize).SetCompression(Compression{}.Gzip()),
					&messagesReceived)

				wg.Add(ThreadCount)
			})

			AfterEach(func() {
				err := producer.Close()
				Expect(err).NotTo(HaveOccurred())
			})

			It("Send batched and subentry messages synchronously", func() {
				sendSynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread, BatchSize)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)
			})

			It("Send batched messages asynchronously", func() {
				sendAsynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)
			})
		})
	})

	It("Not found NotExistingStream", func() {
		_, err := testEnvironment.NewProducer("notExistingStream", nil)
		Expect(err).
			To(Equal(StreamDoesNotExist))
	})

	It("Send Confirmation", func() {
		var messagesReceived int32 = 0

		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			ids := <-ch
			atomic.AddInt32(&messagesReceived, int32(len(ids)))
		}(chConfirm)

		Expect(producer.BatchSend(CreateArrayMessagesForTesting(14))).
			NotTo(HaveOccurred())

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(14)),
			"confirm should receive same messages send by producer")

		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("Handle close", func() {
		var commandIdRecv int32

		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		chClose := producer.NotifyClose()
		go func(ch ChannelClose) {
			event := <-ch
			atomic.StoreInt32(&commandIdRecv, int32(event.Command))
		}(chClose)

		Expect(producer.BatchSend(CreateArrayMessagesForTesting(2))).
			NotTo(HaveOccurred())
		time.Sleep(100 * time.Millisecond)
		Expect(producer.Close()).NotTo(HaveOccurred())
		Eventually(func() int32 {
			return atomic.LoadInt32(&commandIdRecv)
		}, 5*time.Second).Should(Equal(int32(CommandDeletePublisher)),
			"ChannelClose should receive CommandDeletePublisher command")

	})

	It("Pre Publisher errors / Frame too large ", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		var messagesError int32
		chPublishError := producer.NotifyPublishError()
		go func(ch ChannelPublishError) {
			for range ch {
				atomic.AddInt32(&messagesError, 1)
			}
		}(chPublishError)
		Expect(err).NotTo(HaveOccurred())
		var arr []message.StreamMessage
		for z := 0; z < 101; z++ {
			s := make([]byte, 15000)
			arr = append(arr, amqp.NewMessage(s))
		}
		Expect(producer.BatchSend(arr)).To(Equal(FrameTooLarge))

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesError)
		}, 5*time.Second).Should(Equal(int32(101)),
			"invalidate all the messages sent in the batch")

		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("Smart Send/Close", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		var messagesReceived int32
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			for ids := range ch {
				atomic.AddInt32(&messagesReceived, int32(len(ids)))
			}
		}(chConfirm)

		for z := 0; z < 101; z++ {
			s := make([]byte, 50)
			Expect(producer.Send(amqp.NewMessage(s))).NotTo(HaveOccurred())
		}

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(101)),
			"confirm should receive same messages send by producer")

		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		Expect(producer.Close()).NotTo(HaveOccurred())
		// in this case must raise an error since the producer is closed
		Expect(producer.Close()).To(HaveOccurred())
	})

	It("Smart Send Split frame/BatchSize", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchSize(50))
		Expect(err).NotTo(HaveOccurred())
		var messagesReceived int32
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			for ids := range ch {
				atomic.AddInt32(&messagesReceived, int32(len(ids)))
			}
		}(chConfirm)

		for z := 0; z < 2; z++ {
			s := make([]byte, 1048000)
			Expect(producer.Send(amqp.NewMessage(s))).NotTo(HaveOccurred())
		}

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(2)),
			"confirm should receive same messages send by producer")

		By("Max frame Error")
		s := make([]byte, 1148576)
		Expect(producer.Send(amqp.NewMessage(s))).To(HaveOccurred())
		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		Expect(producer.Close()).NotTo(HaveOccurred())

		producer, err = testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchSize(2))
		Expect(err).NotTo(HaveOccurred())
		var messagesConfirmed int32
		chConfirmBatch := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			for ids := range ch {
				atomic.AddInt32(&messagesConfirmed, int32(len(ids)))
			}
		}(chConfirmBatch)

		for i := 0; i < 101; i++ {
			s := make([]byte, 11)
			err = producer.Send(amqp.NewMessage(s))
			Expect(err).NotTo(HaveOccurred())
		}

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesConfirmed)
		}, 5*time.Second).Should(Equal(int32(101)),
			"confirm should receive same messages send by producer")

		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		Expect(producer.Close()).NotTo(HaveOccurred())

	})

	It("Smart Send send after", func() {
		// this test is need to test "send after"
		// and the time check
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		var messagesConfirmed int32
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			for ids := range ch {
				atomic.AddInt32(&messagesConfirmed, int32(len(ids)))
			}
		}(chConfirm)

		for z := 0; z < 5; z++ {
			s := make([]byte, 50)
			Expect(producer.Send(amqp.NewMessage(s))).NotTo(HaveOccurred())
		}

		for z := 0; z < 5; z++ {
			s := make([]byte, 50)
			Expect(producer.Send(amqp.NewMessage(s))).NotTo(HaveOccurred())
		}

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesConfirmed)
		}, 5*time.Second).Should(Equal(int32(10)),
			"confirm should receive same messages send by producer")

		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("Smart Send send after BatchPublishingDelay", func() {
		// this test is need to test "send after BatchPublishingDelay"
		// and the time check
		producer, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchPublishingDelay(50))
		Expect(err).NotTo(HaveOccurred())
		var messagesReceived int32
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			for ids := range ch {
				atomic.AddInt32(&messagesReceived, int32(len(ids)))
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

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(10)),
			"confirm should receive same messages send by producer")

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

		_, err = env.NewProducer(testProducerStream, NewProducerOptions().
			SetBatchSize(0))
		Expect(err).To(HaveOccurred())

		_, err = env.NewProducer(testProducerStream, NewProducerOptions().
			SetBatchSize(20_000))
		Expect(err).To(HaveOccurred())

		_, err = env.NewProducer(testProducerStream, NewProducerOptions().
			SetBatchSize(5_000_000))
		Expect(err).To(HaveOccurred())

		_, err = env.NewProducer(testProducerStream, NewProducerOptions().
			SetBatchPublishingDelay(0))
		Expect(err).To(HaveOccurred())

		_, err = env.NewProducer(testProducerStream, NewProducerOptions().
			SetBatchPublishingDelay(600))
		Expect(err).To(HaveOccurred())

		_, err = env.NewProducer(testProducerStream, NewProducerOptions().
			SetSubEntrySize(0))
		Expect(err).To(HaveOccurred())

		_, err = env.NewProducer(testProducerStream, NewProducerOptions().
			SetSubEntrySize(1).SetCompression(Compression{}.Gzip()))
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

		var messagesConfirmed int32 = 0
		producer, err := testEnvironment.NewProducer(prodErrorStream, nil)
		Expect(err).NotTo(HaveOccurred())
		chPublishError := producer.NotifyPublishError()
		go func(ch ChannelPublishError) {
			for range ch {
				atomic.AddInt32(&messagesConfirmed, 1)
			}
		}(chPublishError)

		var messagesSequence = make([]messageSequence, 1)
		msg := amqp.NewMessage([]byte("test"))
		msg.SetPublishingId(1)
		messageBytes, _ := msg.MarshalBinary()
		messagesSequence[0] = messageSequence{
			messageBytes:     messageBytes,
			unCompressedSize: len(messageBytes),
		}
		for _, producerC := range producer.options.client.coordinator.producers {
			producerC.(*Producer).id = uint8(200)
		}
		producer.options.client.coordinator.mutex.Lock()
		producer.options.client.coordinator.producers[uint8(200)] = producer
		producer.options.client.coordinator.mutex.Unlock()
		// 200 producer ID doesn't exist
		Expect(producer.internalBatchSendProdId(messagesSequence, 200)).
			NotTo(HaveOccurred())

		Expect(env.DeleteStream(prodErrorStream)).NotTo(HaveOccurred())

		producer.options.client.coordinator.mutex.Lock()
		delete(producer.options.client.coordinator.producers, uint8(200))
		delete(producer.options.client.coordinator.producers, uint8(0))
		producer.options.client.coordinator.mutex.Unlock()
		Expect(env.Close()).NotTo(HaveOccurred())

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesConfirmed)
		}, 5*time.Second).ShouldNot(Equal(0),
			"it should receive some message")
	})

	It("Publish Confirm/Send reuse the same message", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().
				SetSubEntrySize(1))
		Expect(err).NotTo(HaveOccurred())
		var messagesConfirmed int32
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			for ids := range ch {
				atomic.AddInt32(&messagesConfirmed, int32(len(ids)))
			}
		}(chConfirm)

		// this test is needed to test if sending the same message
		// there are the different sequences, so send and confirmed
		// must be the same
		msg := amqp.NewMessage(make([]byte, 50))
		for z := 0; z < 232; z++ {
			Expect(producer.Send(msg)).NotTo(HaveOccurred())
		}

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesConfirmed)
		}, 5*time.Second).Should(Equal(int32(232)),
			"confirm should receive same messages send by producer")

		Expect(len(producer.unConfirmedMessages)).To(Equal(0))

		// same test above but using batch send
		var arr []message.StreamMessage
		for i := 0; i < 20; i++ {
			arr = append(arr, amqp.NewMessage(make([]byte, 50)))
		}
		atomic.StoreInt32(&messagesConfirmed, 0)
		for z := 0; z < 12; z++ {
			Expect(producer.BatchSend(arr)).NotTo(HaveOccurred())
		}

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesConfirmed)
		}, 5*time.Second).Should(Equal(int32(12*20)),
			"confirm should receive same messages send by producer")

		Expect(len(producer.unConfirmedMessages)).To(Equal(0))

		Expect(producer.Close()).NotTo(HaveOccurred())

	})

	////  sub-entry batching

	It(" sub-entry batching test Aggregation", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchPublishingDelay(100).
				SetSubEntrySize(77))
		Expect(err).NotTo(HaveOccurred())
		messagesSequence := make([]messageSequence, 201)
		entries, err := producer.aggregateEntities(messagesSequence,
			producer.options.SubEntrySize,
			producer.options.Compression)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(entries.items)).To(Equal(3))

		messagesSequence = make([]messageSequence, 100)
		entries, err = producer.aggregateEntities(messagesSequence,
			producer.options.SubEntrySize,
			producer.options.Compression)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(entries.items)).To(Equal(2))

		messagesSequence = make([]messageSequence, 1)
		entries, err = producer.aggregateEntities(messagesSequence,
			producer.options.SubEntrySize,
			producer.options.Compression)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(entries.items)).To(Equal(1))

		messagesSequence = make([]messageSequence, 1000)
		entries, err = producer.aggregateEntities(messagesSequence,
			producer.options.SubEntrySize,
			producer.options.Compression)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(entries.items)).To(Equal(13))

		messagesSequence = make([]messageSequence, 14)
		entries, err = producer.aggregateEntities(messagesSequence, 13,
			producer.options.Compression)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(entries.items)).To(Equal(2))

		Expect(producer.Close()).NotTo(HaveOccurred())

	})
	It("Sub Size Publish Confirm/Send", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchPublishingDelay(100).
				SetSubEntrySize(77))
		Expect(err).NotTo(HaveOccurred())
		var messagesConfirmed int32
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			for ids := range ch {
				atomic.AddInt32(&messagesConfirmed, int32(len(ids)))
			}
		}(chConfirm)

		for z := 0; z < 501; z++ {
			msg := amqp.NewMessage(make([]byte, 50))
			Expect(producer.Send(msg)).NotTo(HaveOccurred())
		}

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesConfirmed)
		}, 5*time.Second).Should(Equal(int32(501)),
			"confirm should receive same messages send by producer")

		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		atomic.StoreInt32(&messagesConfirmed, 0)

		for z := 0; z < 501; z++ {
			Expect(producer.BatchSend(CreateArrayMessagesForTesting(5))).
				NotTo(HaveOccurred())
		}

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesConfirmed)
		}, 5*time.Second).Should(Equal(int32(501*5)),
			"confirm should receive same messages send by producer")

		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("Sub Size Publish GZIP Confirm/Send", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchPublishingDelay(100).
				SetSubEntrySize(33).SetCompression(Compression{}.Gzip()))
		Expect(err).NotTo(HaveOccurred())
		var messagesConfirmed int32
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			for ids := range ch {
				atomic.AddInt32(&messagesConfirmed, int32(len(ids)))
			}
		}(chConfirm)

		for z := 0; z < 457; z++ {
			msg := amqp.NewMessage(make([]byte, 50))
			Expect(producer.Send(msg)).NotTo(HaveOccurred())
		}

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesConfirmed)
		}, 5*time.Second).Should(Equal(int32(457)),
			"confirm should receive same messages send by producer")

		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		atomic.StoreInt32(&messagesConfirmed, 0)

		for z := 0; z < 457; z++ {
			Expect(producer.BatchSend(CreateArrayMessagesForTesting(5))).
				NotTo(HaveOccurred())
		}

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesConfirmed)
		}, 5*time.Second).Should(Equal(int32(457*5)),
			"confirm should receive same messages send by producer")

		Expect(len(producer.unConfirmedMessages)).To(Equal(0))
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

})

func createProducer(producerOptions *ProducerOptions, messagesReceived *int32) *Producer {
	var err error
	producer, err := testEnvironment.NewProducer(testProducerStream, producerOptions)
	Expect(err).NotTo(HaveOccurred())

	chConfirm := producer.NotifyPublishConfirmation()
	go func(ch ChannelPublishConfirm) {
		ids := <-ch
		atomic.AddInt32(messagesReceived, int32(len(ids)))
	}(chConfirm)

	return producer
}

func sendAsynchronously(producer *Producer, threadCount int, wg *sync.WaitGroup, totalMessageCountPerThread int) {
	runConcurrentlyAndWaitTillAllDone(threadCount, wg, func(goRoutingIndex int) {
		messagePrefix := fmt.Sprintf("test_%d_", goRoutingIndex)
		for i := 0; i < totalMessageCountPerThread; i++ {
			Expect(producer.Send(CreateMessageForTesting(messagePrefix, i))).NotTo(HaveOccurred())
		}
	})
}

func sendSynchronously(producer *Producer, threadCount int, wg *sync.WaitGroup, totalMessageCountPerThread int, batchSize int) {
	runConcurrentlyAndWaitTillAllDone(threadCount, wg, func(goRoutingIndex int) {
		totalBatchCount := totalMessageCountPerThread / batchSize
		for batchIndex := 0; batchIndex < totalBatchCount; batchIndex++ {
			messagePrefix := fmt.Sprintf("test_%d_%d_", goRoutingIndex, batchIndex)
			Expect(producer.BatchSend(CreateArrayMessagesForTestingWithPrefix(messagePrefix, batchSize))).NotTo(HaveOccurred())
		}
	})
}

func verifyProducerSent(producer *Producer, messageReceived *int32, messageSent int) {
	verifyMessageWereConfirmed(messageReceived, messageSent)
	Expect(len(producer.unConfirmedMessages)).To(Equal(0))
}

func verifyMessageWereConfirmed(actualMessageCount *int32, expectedMessageCount int) {

	Eventually(func() int32 {
		return atomic.LoadInt32(actualMessageCount)
	}, 5*time.Second).Should(Equal(int32(expectedMessageCount)),
		"confirm should receive same messages send by producer")
}

func runConcurrentlyAndWaitTillAllDone(threadCount int, wg *sync.WaitGroup, runner func(int)) {
	for index := 0; index < threadCount; index++ {
		go func(i int) {
			defer wg.Done()
			runner(i)
		}(index)
	}
	wg.Wait()
}
