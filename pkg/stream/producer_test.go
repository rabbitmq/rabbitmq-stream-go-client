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
				producer = createProducer(NewProducerOptions().SetBatchSize(1).SetSubEntrySize(1),
					&messagesReceived)
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
				producer = createProducer(NewProducerOptions().SetBatchSize(BatchSize).SetSubEntrySize(1),
					&messagesReceived)
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
					NewProducerOptions().SetBatchSize(BatchSize).SetSubEntrySize(SubEntrySize).SetCompression(Compression{}.None()),
					&messagesReceived)
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
					&messagesReceived)

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

	It("Wait for inflight messages", func() {
		// https://github.com/rabbitmq/rabbitmq-stream-go-client/issues/103

		producer, err := testEnvironment.NewProducer(testProducerStream, nil)
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 65672; i++ {
			Expect(producer.Send(amqp.NewMessage([]byte("h")))).NotTo(HaveOccurred())
		}

		Expect(producer.Close()).NotTo(HaveOccurred())
		Expect(producer.lenPendingMessages()).To(Equal(0))
		Expect(len(producer.messageSequenceCh)).To(Equal(0))
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

	It("Already Closed/Limits", func() {
		env, err := NewEnvironment(NewEnvironmentOptions())
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
	// In order to simulate the producer id not found I need to
	// change manually the producer id.
	// It works, but would be better to introduce some mock function
	It("Publish  Error", func() {
		//TODO: Implement this test
		env, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		prodErrorStream := uuid.New().String()
		err = env.DeclareStream(prodErrorStream, nil)
		Expect(err).NotTo(HaveOccurred())

		var messagesConfirmed int32 = 0
		producer, err := testEnvironment.NewProducer(prodErrorStream, nil)
		Expect(err).NotTo(HaveOccurred())

		var messagesSequence = make([]messageSequence, 1)
		msg := amqp.NewMessage([]byte("test"))
		msg.SetPublishingId(1)
		messageBytes, _ := msg.MarshalBinary()
		messagesSequence[0] = messageSequence{
			messageBytes:     messageBytes,
			unCompressedSize: len(messageBytes),
		}

		// 200 producer ID doesn't exist
		Expect(producer.internalBatchSendProdId(messagesSequence, 200)).
			NotTo(HaveOccurred())

		Expect(env.DeleteStream(prodErrorStream)).NotTo(HaveOccurred())
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
		go func(ch chan ProducerResponse) {
			for r := range ch {
				atomic.AddInt32(&messagesConfirmed, int32(len(r.GetListOfConfirmations())))
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
		go func(ch chan ProducerResponse) {
			for r := range ch {
				atomic.AddInt32(&messagesConfirmed, int32(len(r.GetListOfConfirmations())))
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

		atomic.StoreInt32(&messagesConfirmed, 0)

		for z := 0; z < 501; z++ {
			Expect(producer.BatchSend(CreateArrayMessagesForTesting(5))).
				NotTo(HaveOccurred())
		}

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesConfirmed)
		}, 5*time.Second).Should(Equal(int32(501*5)),
			"confirm should receive same messages send by producer")

		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("Sub Size Publish Compression Confirm/Send", func() {
		producerGZIP, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchPublishingDelay(100).
				SetSubEntrySize(33).SetCompression(Compression{}.Gzip()))
		Expect(err).NotTo(HaveOccurred())
		testCompress(producerGZIP)
		Expect(producerGZIP.Close()).NotTo(HaveOccurred())

		producerLz4, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchPublishingDelay(100).
				SetSubEntrySize(55).SetCompression(Compression{}.Lz4()))
		Expect(err).NotTo(HaveOccurred())
		testCompress(producerLz4)
		Expect(producerLz4.Close()).NotTo(HaveOccurred())

		producerSnappy, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchPublishingDelay(50).
				SetSubEntrySize(666).SetCompression(Compression{}.Snappy()))
		Expect(err).NotTo(HaveOccurred())
		testCompress(producerSnappy)
		Expect(producerSnappy.Close()).NotTo(HaveOccurred())

		producerZstd, err := testEnvironment.NewProducer(testProducerStream,
			NewProducerOptions().SetBatchPublishingDelay(200).
				SetSubEntrySize(98).SetCompression(Compression{}.Zstd()))
		Expect(err).NotTo(HaveOccurred())
		testCompress(producerZstd)
		Expect(producerZstd.Close()).NotTo(HaveOccurred())

	})

})

func testCompress(producer *Producer) {
	var messagesConfirmed int32
	chConfirm := producer.NotifyPublishConfirmation()
	go func(ch chan ProducerResponse) {
		for r := range ch {
			atomic.AddInt32(&messagesConfirmed, int32(len(r.GetListOfConfirmations())))
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

	atomic.StoreInt32(&messagesConfirmed, 0)

	for z := 0; z < 457; z++ {
		Expect(producer.BatchSend(CreateArrayMessagesForTesting(5))).
			NotTo(HaveOccurred())
	}

	Eventually(func() int32 {
		return atomic.LoadInt32(&messagesConfirmed)
	}, 5*time.Second).Should(Equal(int32(457*5)),
		"confirm should receive same messages send by producer")
}

func createProducer(producerOptions *ProducerOptions, messagesReceived *int32) *Producer {
	var err error

	atomic.StoreInt32(messagesReceived, 0)

	producer, err := testEnvironment.NewProducer(testProducerStream, producerOptions)
	Expect(err).NotTo(HaveOccurred())

	chConfirm := producer.NotifyPublishConfirmation()
	go func(ch chan ProducerResponse) {
		for r := range ch {
			atomic.AddInt32(messagesReceived, int32(len(r.GetListOfConfirmations())))
		}
	}(chConfirm)

	return producer
}

func sendConcurrentlyAndAsynchronously(producer *Producer, threadCount int, wg *sync.WaitGroup, totalMessageCountPerThread int) {
	runConcurrentlyAndWaitTillAllDone(threadCount, wg, func(goRoutingIndex int) {
		//fmt.Printf("[%d] Sending %d messages asynchronoulsy\n", goRoutingIndex, totalMessageCountPerThread)
		messagePrefix := fmt.Sprintf("test_%d_", goRoutingIndex)
		for i := 0; i < totalMessageCountPerThread; i++ {
			Expect(producer.Send(CreateMessageForTesting(messagePrefix, i))).NotTo(HaveOccurred())
		}
		//fmt.Printf("[%d] Sent %d messages\n", goRoutingIndex, totalMessageCountPerThread)

	})
}

func sendConcurrentlyAndSynchronously(producer *Producer, threadCount int, wg *sync.WaitGroup, totalMessageCountPerThread int, batchSize int) {
	runConcurrentlyAndWaitTillAllDone(threadCount, wg, func(goRoutingIndex int) {
		totalBatchCount := totalMessageCountPerThread / batchSize
		//fmt.Printf("[%d] Sending %d messages in batches of %d (total batch:%d) synchronously\n", goRoutingIndex,
		//			totalMessageCountPerThread, batchSize, totalBatchCount)
		for batchIndex := 0; batchIndex < totalBatchCount; batchIndex++ {
			messagePrefix := fmt.Sprintf("test_%d_%d_", goRoutingIndex, batchIndex)
			Expect(producer.BatchSend(CreateArrayMessagesForTestingWithPrefix(messagePrefix, batchSize))).NotTo(HaveOccurred())
		}
		//fmt.Printf("[%d] Sent %d messages\n", goRoutingIndex, totalMessageCountPerThread)

	})
}

func verifyProducerSent(producer *Producer, confirmationReceived *int32, messageSent int) {
	fmt.Printf("Waiting for %d confirmations ...\n", messageSent)
	Eventually(func() int32 {
		return atomic.LoadInt32(confirmationReceived)
	}, 10*time.Second, 1*time.Second).Should(Equal(int32(messageSent)),
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
	//fmt.Printf("Finished running concurrently with %d threads\n", threadCount)
}
