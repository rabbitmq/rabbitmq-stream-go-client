package stream

import (
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"sync"
	"sync/atomic"
	"time"
)

var _ = Describe("Streaming Producers", func() {
	var (
		testEnvironment    *Environment
		testProducerStream string
	)

	BeforeEach(func() {
		client, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		testEnvironment = client
		testProducerStream = uuid.New().String()
		Expect(testEnvironment.DeclareStream(testProducerStream, nil)).
			NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(testEnvironment.DeleteStream(testProducerStream)).NotTo(HaveOccurred())
		Expect(testEnvironment.Close()).To(Succeed())
		Eventually(testEnvironment.IsClosed, time.Millisecond*300).Should(BeTrue(), "Expected testEnvironment to be closed")
	})

	It("NewProducer/Close Publisher", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("NewProducer/Send/Close Publisher", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		err = producer.BatchSend(CreateArrayMessagesForTesting(5))
		Expect(err).NotTo(HaveOccurred())
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("Multi-thread newProducer/Send", func() {
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(wg *sync.WaitGroup) {
				defer GinkgoRecover()
				defer wg.Done()
				producer, err := testEnvironment.NewProducer(testProducerStream, nil)
				Expect(err).NotTo(HaveOccurred())

				err = producer.BatchSend(CreateArrayMessagesForTesting(5))
				Expect(err).NotTo(HaveOccurred())

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
					NewProducerOptions().SetBatchSize(1).SetSubEntrySize(1),
					&messagesReceived,
					testEnvironment,
					testProducerStream,
				)
				wg.Add(ThreadCount)
			})

			AfterEach(func() {
				err := producer.Close()
				Expect(err).NotTo(HaveOccurred())
			})

			It("Send plain messages synchronously", func() {
				sendConcurrentlyAndSynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread, 1)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)

			})
			It("Send plain messages asynchronously", func() {
				sendConcurrentlyAndAsynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)
			})

		})
		When("Batching enabled, but without sub-entry and compression", func() {

			BeforeEach(func() {
				producer = createProducer(
					NewProducerOptions().SetBatchSize(BatchSize).SetSubEntrySize(1),
					&messagesReceived,
					testEnvironment,
					testProducerStream,
				)
				wg.Add(ThreadCount)
			})

			AfterEach(func() {
				err := producer.Close()
				Expect(err).NotTo(HaveOccurred())
			})

			It("Send batched messages synchronously", func() {
				sendConcurrentlyAndSynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread, BatchSize)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)
			})

			It("Send batched messages asynchronously", func() {
				sendConcurrentlyAndAsynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)
			})
		})

		When("Batching and sub-entry enabled without compression", func() {

			BeforeEach(func() {
				producer = createProducer(
					NewProducerOptions().
						SetBatchSize(BatchSize).
						SetSubEntrySize(SubEntrySize).
						SetClientProvidedName("batch-go-stream-producer").
						SetConfirmationTimeOut(1*time.Second).
						SetCompression(Compression{}.None()),
					&messagesReceived,
					testEnvironment,
					testProducerStream,
				)
				wg.Add(ThreadCount)
			})

			AfterEach(func() {
				err := producer.Close()
				Expect(err).NotTo(HaveOccurred())
			})

			It("Send batched and subentry messages synchronously", func() {
				sendConcurrentlyAndSynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread, BatchSize)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)
			})

			It("Send batched messages asynchronously", func() {
				sendConcurrentlyAndAsynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)
			})
		})

		When("Batching and sub-entry enabled with GZIP compression", func() {

			BeforeEach(func() {
				producer = createProducer(
					NewProducerOptions().SetBatchSize(BatchSize).SetSubEntrySize(SubEntrySize).SetCompression(Compression{}.Gzip()),
					&messagesReceived,
					testEnvironment,
					testProducerStream,
				)

				wg.Add(ThreadCount)
			})

			AfterEach(func() {
				err := producer.Close()
				Expect(err).NotTo(HaveOccurred())
			})

			It("Send batched and subentry messages synchronously", func() {
				sendConcurrentlyAndSynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread, BatchSize)
				verifyProducerSent(producer, &messagesReceived, TotalMessageCountPerThread*ThreadCount)
			})

			It("Send batched messages asynchronously", func() {
				sendConcurrentlyAndAsynchronously(producer, ThreadCount, &wg, TotalMessageCountPerThread)
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
		// we need an external nRecv since the producer can Send the confirmation
		// in multiple Send
		var nRecv int64
		go func(ch ChannelPublishConfirm, p *Producer) {
			defer GinkgoRecover()
			for ids := range ch {
				for _, msg := range ids {
					atomic.AddInt64(&nRecv, 1)
					Expect(msg.GetError()).NotTo(HaveOccurred())
					Expect(msg.GetProducerID()).To(Equal(p.id))
					Expect(msg.GetPublishingId()).To(Equal(atomic.LoadInt64(&nRecv)))
					Expect(msg.IsConfirmed()).To(Equal(true))
					Expect(msg.message.GetPublishingId()).To(Equal(int64(0)))
					Expect(msg.message.HasPublishingId()).To(Equal(false))
					body := string(msg.message.GetData()[0][:])
					// -1 because the first message is 0
					be := fmt.Sprintf("test_%d", atomic.LoadInt64(&nRecv)-1)
					Expect(body).To(Equal(be))
				}
				atomic.AddInt32(&messagesReceived, int32(len(ids)))
			}
		}(chConfirm, producer)

		err = producer.BatchSend(CreateArrayMessagesForTesting(14))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(14)),
			"confirm should receive same messages Send by producer")

		Expect(producer.lenUnConfirmed()).To(Equal(0))
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("Wait for inflight messages", func() {
		// https://github.com/rabbitmq/rabbitmq-stream-go-client/issues/103

		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 65672; i++ {
			Expect(producer.Send(amqp.NewMessage([]byte("h")))).NotTo(HaveOccurred())
		}

		Expect(producer.Close()).NotTo(HaveOccurred())
		Expect(producer.lenUnConfirmed()).To(Equal(0))
		Expect(producer.pendingSequencesQueue.IsEmpty()).To(Equal(true))
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

		err = producer.BatchSend(CreateArrayMessagesForTesting(2))
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(100 * time.Millisecond)
		Expect(producer.Close()).NotTo(HaveOccurred())
		Eventually(func() int32 {
			return atomic.LoadInt32(&commandIdRecv)
		}, 5*time.Second).Should(Equal(int32(CommandDeletePublisher)),
			"ChannelClose should receive CommandDeletePublisher command")

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
			"confirm should receive same messages Send by producer")

		Expect(producer.lenUnConfirmed()).To(Equal(0))
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
		}, 5*time.Second).WithPolling(200*time.Millisecond).Should(Equal(int32(2)),
			"confirm should receive same messages Send by producer")

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
			"confirm should receive same messages Send by producer")

		Expect(producer.lenUnConfirmed()).To(Equal(0))
		Expect(producer.Close()).NotTo(HaveOccurred())

	})

	It("Smart Send Send after", func() {
		// this test is need to test "Send after"
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
			"confirm should receive same messages Send by producer")

		Expect(producer.lenUnConfirmed()).To(Equal(0))
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("BatchSend should not a send a big message", func() {
		// 1.5 Milestone
		// the batch send should not send a big message
		// The message should be sed back to the client with an error
		// FrameTooLarge and not confirmed
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		var notConfirmedTooLarge int32
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			for ids := range ch {
				for _, conf := range ids {
					if !conf.IsConfirmed() {
						Expect(conf.GetError()).To(Equal(FrameTooLarge))
						atomic.AddInt32(&notConfirmedTooLarge, 1)
					}
				}
			}
		}(chConfirm)
		err = producer.BatchSend([]message.StreamMessage{amqp.NewMessage(make([]byte, MessageBufferTooBig))})
		Expect(err).To(HaveOccurred())
		Eventually(func() int32 {
			return atomic.LoadInt32(&notConfirmedTooLarge)
		}).Should(Equal(int32(1)))
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("Send should not a send a big message", func() {
		// 1.5 Milestone
		// the Send() method should not send a big message
		// The message should be sed back to the client with an error
		// FrameTooLarge and not confirmed
		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		var notConfirmedTooLarge int32
		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			defer GinkgoRecover()
			for ids := range ch {
				for _, conf := range ids {
					if !conf.IsConfirmed() {
						Expect(conf.GetError()).To(Equal(FrameTooLarge))
						atomic.AddInt32(&notConfirmedTooLarge, 1)
					}
				}
			}
		}(chConfirm)
		err = producer.Send(amqp.NewMessage(make([]byte, MessageBufferTooBig)))
		Expect(err).To(HaveOccurred())
		Eventually(func() int32 {
			return atomic.LoadInt32(&notConfirmedTooLarge)
		}, 5*time.Second).Should(Equal(int32(1)))
		Expect(producer.Close()).NotTo(HaveOccurred())
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

		_, err = env.NewProducer(testProducerStream, &ProducerOptions{
			SubEntrySize: 65_539,
		})
		Expect(err).To(HaveOccurred())

		err = env.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	// this test is needed to test publish error.
	// In order to simulate the producer id not found
	It("Publish  Error", func() {
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		prodErrorStream := uuid.New().String()
		err = env.DeclareStream(prodErrorStream, nil)
		Expect(err).NotTo(HaveOccurred())

		var messagesConfirmed int32 = 0
		producer, err := testEnvironment.NewProducer(prodErrorStream, nil)
		Expect(err).NotTo(HaveOccurred())
		chPublishError := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm) {
			defer GinkgoRecover()
			for msgs := range ch {
				for _, msg := range msgs {
					if !msg.IsConfirmed() {
						Expect(msg.GetError()).To(Equal(PublisherDoesNotExist))
						atomic.AddInt32(&messagesConfirmed, 1)
					}
				}
			}
		}(chPublishError)

		var messagesSequence = make([]*messageSequence, 1)

		for i := 0; i < 1; i++ {
			s := make([]byte, 50)
			messagesSequence[i] = &messageSequence{
				messageBytes:     s,
				unCompressedSize: len(s),
			}
		}

		msg := amqp.NewMessage([]byte("test"))
		msg.SetPublishingId(1)
		messageBytes, _ := msg.MarshalBinary()
		messagesSequence[0] = &messageSequence{
			messageBytes:     messageBytes,
			unCompressedSize: len(messageBytes),
		}

		// 200 producer ID doesn't exist
		Expect(producer.internalBatchSendProdId(messagesSequence, 200)).
			NotTo(HaveOccurred())

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesConfirmed)
		}, 5*time.Second).ShouldNot(Equal(0),
			"it should receive some message")

		Expect(env.DeleteStream(prodErrorStream)).NotTo(HaveOccurred())
		Expect(env.Close()).NotTo(HaveOccurred())

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
		// there are the different sequences, so Send and confirmed
		// must be the same
		msg := amqp.NewMessage(make([]byte, 50))
		for z := 0; z < 232; z++ {
			Expect(producer.Send(msg)).NotTo(HaveOccurred())
		}

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesConfirmed)
		}, 5*time.Second).Should(Equal(int32(232)),
			"confirm should receive same messages Send by producer")

		Expect(producer.lenUnConfirmed()).To(Equal(0))

		// same test above but using batch Send
		var arr []message.StreamMessage
		for i := 0; i < 20; i++ {
			arr = append(arr, amqp.NewMessage(make([]byte, 50)))
		}
		atomic.StoreInt32(&messagesConfirmed, 0)
		for z := 0; z < 12; z++ {
			err := producer.BatchSend(arr)
			Expect(err).NotTo(HaveOccurred())
		}

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesConfirmed)
		}, 5*time.Second).Should(Equal(int32(12*20)),
			"confirm should receive same messages Send by producer")

		Expect(producer.lenUnConfirmed()).To(Equal(0))

		Expect(producer.Close()).NotTo(HaveOccurred())

	})

	////  sub-entry batching

	It(" sub-entry batching test Aggregation", func() {
		producer, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchPublishingDelay(100).
				SetSubEntrySize(77))
		Expect(err).NotTo(HaveOccurred())
		messagesSequence := make([]*messageSequence, 201)

		for i := 0; i < 201; i++ {
			s := make([]byte, 50)
			messagesSequence[i] = &messageSequence{
				messageBytes:     s,
				unCompressedSize: len(s),
			}
		}

		entries, err := producer.aggregateEntities(messagesSequence,
			producer.options.SubEntrySize,
			producer.options.Compression)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(entries.items)).To(Equal(3))

		messagesSequence = make([]*messageSequence, 100)

		for i := 0; i < 100; i++ {

			s := make([]byte, 50)
			messagesSequence[i] = &messageSequence{
				messageBytes:     s,
				unCompressedSize: len(s),
			}
		}

		entries, err = producer.aggregateEntities(messagesSequence,
			producer.options.SubEntrySize,
			producer.options.Compression)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(entries.items)).To(Equal(2))

		messagesSequence = make([]*messageSequence, 1)

		for i := 0; i < 1; i++ {
			s := make([]byte, 50)
			messagesSequence[i] = &messageSequence{
				messageBytes:     s,
				unCompressedSize: len(s),
			}
		}

		entries, err = producer.aggregateEntities(messagesSequence,
			producer.options.SubEntrySize,
			producer.options.Compression)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(entries.items)).To(Equal(1))

		messagesSequence = make([]*messageSequence, 1000)

		for i := 0; i < 1000; i++ {
			s := make([]byte, 50)
			messagesSequence[i] = &messageSequence{
				messageBytes:     s,
				unCompressedSize: len(s),
			}
		}

		entries, err = producer.aggregateEntities(messagesSequence,
			producer.options.SubEntrySize,
			producer.options.Compression)
		Expect(err).NotTo(HaveOccurred())
		Expect(len(entries.items)).To(Equal(13))

		messagesSequence = make([]*messageSequence, 14)

		for i := 0; i < 14; i++ {
			s := make([]byte, 50)
			messagesSequence[i] = &messageSequence{
				messageBytes:     s,
				unCompressedSize: len(s),
			}
		}

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
			"confirm should receive same messages Send by producer")

		Expect(producer.lenUnConfirmed()).To(Equal(0))
		atomic.StoreInt32(&messagesConfirmed, 0)

		for z := 0; z < 501; z++ {
			err := producer.BatchSend(CreateArrayMessagesForTesting(5))
			Expect(err).NotTo(HaveOccurred())
		}

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesConfirmed)
		}, 5*time.Second).Should(Equal(int32(501*5)),
			"confirm should receive same messages Send by producer")

		Expect(producer.lenUnConfirmed()).To(Equal(0))
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("Sub Size Publish Compression Confirm/Send", func() {
		producerGZIP, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchPublishingDelay(100).
				SetSubEntrySize(33).SetCompression(Compression{}.Gzip()))
		Expect(err).NotTo(HaveOccurred())
		testCompress(producerGZIP)
		Expect(producerGZIP.lenUnConfirmed()).To(Equal(0))
		Expect(producerGZIP.Close()).NotTo(HaveOccurred())

		producerLz4, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchPublishingDelay(100).
				SetSubEntrySize(55).SetCompression(Compression{}.Lz4()))
		Expect(err).NotTo(HaveOccurred())
		testCompress(producerLz4)
		Expect(producerLz4.lenUnConfirmed()).To(Equal(0))
		Expect(producerLz4.Close()).NotTo(HaveOccurred())

		producerSnappy, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchPublishingDelay(50).
				SetSubEntrySize(666).SetCompression(Compression{}.Snappy()))
		Expect(err).NotTo(HaveOccurred())
		testCompress(producerSnappy)
		Expect(producerSnappy.lenUnConfirmed()).To(Equal(0))
		Expect(producerSnappy.Close()).NotTo(HaveOccurred())

		producerZstd, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchPublishingDelay(200).
				SetSubEntrySize(98).SetCompression(Compression{}.Zstd()))
		Expect(err).NotTo(HaveOccurred())
		testCompress(producerZstd)
		Expect(producerZstd.lenUnConfirmed()).To(Equal(0))
		Expect(producerZstd.Close()).NotTo(HaveOccurred())

	})

	It("Can't send message if the producer is closed", func() {

		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(producer.Close()).NotTo(HaveOccurred())
		err = producer.Send(amqp.NewMessage(make([]byte, 50)))
		Expect(err).To(HaveOccurred())
	})

})

func testCompress(producer *Producer) {
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
		"confirm should receive same messages Send by producer")

	Expect(producer.lenUnConfirmed()).To(Equal(0))
	atomic.StoreInt32(&messagesConfirmed, 0)

	for z := 0; z < 457; z++ {
		err := producer.BatchSend(CreateArrayMessagesForTesting(5))
		Expect(err).NotTo(HaveOccurred())

	}

	Eventually(func() int32 {
		return atomic.LoadInt32(&messagesConfirmed)
	}, 5*time.Second).Should(Equal(int32(457*5)),
		"confirm should receive same messages Send by producer")
}

func createProducer(producerOptions *ProducerOptions, messagesReceived *int32, testEnvironment *Environment, streamName string) *Producer {
	var err error

	atomic.StoreInt32(messagesReceived, 0)

	producer, err := testEnvironment.NewProducer(streamName, producerOptions)
	Expect(err).NotTo(HaveOccurred())

	chConfirm := producer.NotifyPublishConfirmation()
	go func(ch ChannelPublishConfirm) {
		for ids := range ch {
			atomic.AddInt32(messagesReceived, int32(len(ids)))
		}
	}(chConfirm)

	return producer
}

func sendConcurrentlyAndAsynchronously(producer *Producer, threadCount int, wg *sync.WaitGroup, totalMessageCountPerThread int) {
	runConcurrentlyAndWaitTillAllDone(threadCount, wg, func(goRoutingIndex int) {
		defer GinkgoRecover()
		messagePrefix := fmt.Sprintf("test_%d_", goRoutingIndex)
		for i := 0; i < totalMessageCountPerThread; i++ {
			Expect(producer.Send(CreateMessageForTesting(messagePrefix, i))).NotTo(HaveOccurred())
		}
	})
}

func sendConcurrentlyAndSynchronously(producer *Producer, threadCount int, wg *sync.WaitGroup, totalMessageCountPerThread int, batchSize int) {
	runConcurrentlyAndWaitTillAllDone(threadCount, wg, func(goRoutingIndex int) {
		defer GinkgoRecover()
		totalBatchCount := totalMessageCountPerThread / batchSize
		for batchIndex := 0; batchIndex < totalBatchCount; batchIndex++ {
			messagePrefix := fmt.Sprintf("test_%d_%d_", goRoutingIndex, batchIndex)
			err := producer.BatchSend(CreateArrayMessagesForTestingWithPrefix(messagePrefix, batchSize))
			Expect(err).NotTo(HaveOccurred())
		}
	})
}

func verifyProducerSent(producer *Producer, confirmationReceived *int32, messageSent int) {
	fmt.Printf("Waiting for %d confirmations ...\n", messageSent)
	Eventually(func() int32 {
		return atomic.LoadInt32(confirmationReceived)
	}, 10*time.Second, 1*time.Second).Should(Equal(int32(messageSent)),
		"confirm should receive same messages Send by producer")

	Expect(producer.lenUnConfirmed()).To(Equal(0))
}

func runConcurrentlyAndWaitTillAllDone(threadCount int, wg *sync.WaitGroup, runner func(int)) {
	for index := 0; index < threadCount; index++ {
		go func(i int) {
			defer wg.Done()
			runner(i)
		}(index)
	}
	wg.Wait()
	//fmt.Printf("Finished running concurrently with %d threads\n", threadCount)
}
