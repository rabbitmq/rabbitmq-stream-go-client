package stream

import (
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	test_helper "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/test-helper"
)

var _ = Describe("Streaming Consumers", func() {
	var (
		env        *Environment
		streamName string
	)
	BeforeEach(func() {
		testEnv, err := NewEnvironment(nil)
		env = testEnv
		Expect(err).NotTo(HaveOccurred())
		streamName = uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
	})
	AfterEach(func() {
		Expect(env.DeleteStream(streamName)).NotTo(HaveOccurred())

	})

	It("Multi Consumers", func() {
		var consumers []*Consumer
		for range 10 {
			consumer, err := env.NewConsumer(streamName,
				func(_ ConsumerContext, _ *amqp.Message) {}, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(consumer.id).To(Equal(uint8(0)))
			consumers = append(consumers, consumer)
		}

		Expect(len(env.consumers.getCoordinators())).To(Equal(1))
		coordinator, ok := env.consumers.getCoordinators()["localhost:5552"]
		Expect(ok).To(BeTrue())
		Expect(coordinator).NotTo(BeNil())
		Expect(len(coordinator.getClientsPerContext())).To(Equal(10))

		for _, consumer := range consumers {
			Expect(consumer.Close()).NotTo(HaveOccurred())
		}
		// time.Sleep(1 * time.Second)
		Eventually(func() int {
			coordinator, ok := env.consumers.getCoordinators()["localhost:5552"]
			if !ok || coordinator == nil {
				return 0
			}
			return len(coordinator.getClientsPerContext())
		}).ProbeEvery(100 * time.Millisecond).WithTimeout(5 * time.Second).Should(Equal(0))

	})

	It("Multi Consumers per client", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().
			SetMaxConsumersPerClient(2))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		Expect(env.DeclareStream(streamName, nil)).
			NotTo(HaveOccurred())

		for i := 0; i < 10; i++ {
			consumer, err := env.NewConsumer(streamName,
				func(_ ConsumerContext, _ *amqp.Message) {}, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(consumer.id).To(Equal(uint8(i % 2)))
		}

		Expect(env.DeleteStream(streamName)).
			NotTo(HaveOccurred())

	})

	It("Subscribe and Unsubscribe", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().
			SetMaxConsumersPerClient(2))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		Expect(env.DeclareStream(streamName, nil)).
			NotTo(HaveOccurred())
		consumer, err := env.NewConsumer(streamName,
			func(_ ConsumerContext, _ *amqp.Message) {}, nil)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(10 * time.Millisecond)
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("Subscribe fail not exist", func() {
		_, err := env.NewConsumer("NOT_EXIST",
			func(_ ConsumerContext, _ *amqp.Message) {}, nil)

		Expect(errors.Cause(err)).To(Equal(StreamDoesNotExist))
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("Consumer close handler unSubscribe", func() {
		var commandIdRecv int32

		consumer, err := env.NewConsumer(streamName,
			func(_ ConsumerContext, _ *amqp.Message) {}, nil)
		Expect(err).NotTo(HaveOccurred())
		chConsumerClose := consumer.NotifyClose()
		go func(ch ChannelClose) {
			event := <-ch
			atomic.AddInt32(&commandIdRecv, int32(event.Command))
		}(chConsumerClose)
		time.Sleep(100 * time.Millisecond)
		Expect(consumer.Close()).NotTo(HaveOccurred())

		Eventually(func() int32 {
			return atomic.LoadInt32(&commandIdRecv)
		}, 5*time.Second).Should(Equal(int32(CommandUnsubscribe)),
			"command received should be unSubscribe ")

		Expect(err).NotTo(HaveOccurred())
	})

	It("Consumer close handler meta data", func() {
		var commandIdRecv int32
		streamName := uuid.New().String()
		Expect(env.DeclareStream(streamName, nil)).NotTo(HaveOccurred())
		consumer, err := env.NewConsumer(streamName, func(_ ConsumerContext, _ *amqp.Message) {}, nil)
		Expect(err).NotTo(HaveOccurred())
		chConsumerClose := consumer.NotifyClose()
		go func(ch ChannelClose) {
			event := <-ch
			atomic.AddInt32(&commandIdRecv, int32(event.Command))
		}(chConsumerClose)
		time.Sleep(100 * time.Millisecond)
		Expect(err).NotTo(HaveOccurred())
		Expect(env.DeleteStream(streamName)).NotTo(HaveOccurred())
		Eventually(func() int32 {
			return atomic.LoadInt32(&commandIdRecv)
		}, 5*time.Second).Should(Equal(int32(CommandMetadataUpdate)),
			"command received should be CommandMetadataUpdate ")

	})

	It("Subscribe/Unsubscribe count messages SetOffset", func() {
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		err = producer.BatchSend(CreateArrayMessagesForTesting(30)) // batch Send
		Expect(err).NotTo(HaveOccurred())

		Expect(producer.Close()).NotTo(HaveOccurred())
		var messagesReceived int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(_ ConsumerContext, _ *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)
			}, NewConsumerOptions().
				SetOffset(OffsetSpecification{}.Offset(20)).
				SetClientProvidedName("consumer_test").
				SetCRCCheck(true))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(10)),
			"consumer should only 10 messages due the offset 20")

		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("Check the Chunk info", func() {
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		var messagesCount int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, _ *amqp.Message) {
				atomic.SwapInt32(&messagesCount, int32(consumerContext.GetEntriesCount()))
			}, NewConsumerOptions().
				SetOffset(OffsetSpecification{}.First()))
		Expect(err).NotTo(HaveOccurred())
		err = producer.BatchSend(CreateArrayMessagesForTesting(3)) // batch Send
		Expect(err).NotTo(HaveOccurred())

		Expect(producer.Close()).NotTo(HaveOccurred())
		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesCount)
		}, 5*time.Second).Should(Equal(int32(3)),
			"chunkInfo should be 3")
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	Describe("Committing consumed messages", func() {
		BeforeEach(func() {
			producer, err := env.NewProducer(streamName, nil)
			Expect(err).NotTo(HaveOccurred())

			// Given we have produced 105 messages ...
			err = producer.BatchSend(CreateArrayMessagesForTesting(105)) // batch Send
			Expect(err).NotTo(HaveOccurred())

			Expect(producer.Close()).NotTo(HaveOccurred())

		})

		It("can commit a given offset", func() {
			consumer, err := env.NewConsumer(streamName,
				func(consumerContext ConsumerContext, _ *amqp.Message) {
					offset := consumerContext.Consumer.GetOffset()
					if offset%10 == 0 { //  every 10 messages
						err := consumerContext.Consumer.StoreCustomOffset(offset - 1) // commit all except the last one
						if err != nil {
							Fail(err.Error())
						}
					}
				}, NewConsumerOptions().
					SetOffset(OffsetSpecification{}.First()).
					SetConsumerName("my_manual_consumer").
					SetManualCommit().
					SetCRCCheck(false))
			Expect(err).NotTo(HaveOccurred())
			Eventually(func() (int64, error) { return consumer.QueryOffset() }, 5*time.Second).Should(Equal(int64(99)),
				"Offset should be 99")
			Expect(consumer.Close()).NotTo(HaveOccurred())
		})

		It("automatically commit by number/time", func() {

			consumer, err := env.NewConsumer(streamName,
				func(_ ConsumerContext, _ *amqp.Message) {
				}, NewConsumerOptions().
					SetOffset(OffsetSpecification{}.First()).
					SetConsumerName("my_auto_consumer").
					SetCRCCheck(false).
					SetAutoCommit(NewAutoCommitStrategy().
						SetCountBeforeStorage(100).
						SetFlushInterval(50*time.Second))) // here we set a high value to do not trigger the time
			Expect(err).NotTo(HaveOccurred())
			time.Sleep(500 * time.Millisecond)
			Eventually(func() (int64, error) {
				v, err := consumer.QueryOffset()
				// we can ignore the offset not found error here
				if err != nil {
					return 0, nil
				}
				return v, err
				// 99 is the offset since it starts from 0
			}, 5*time.Second).WithPolling(500*time.Millisecond).Should(Equal(int64(99)),
				"Offset should be 99")
			Expect(consumer.Close()).NotTo(HaveOccurred())

			consumerTimer, errTimer := env.NewConsumer(streamName,
				func(_ ConsumerContext, _ *amqp.Message) {
				}, NewConsumerOptions().
					SetOffset(OffsetSpecification{}.First()).
					SetConsumerName("my_auto_consumer_timer").
					SetCRCCheck(true).
					SetAutoCommit(NewAutoCommitStrategy().
						SetCountBeforeStorage(10000000). /// We avoid raising the timer
						SetFlushInterval(1*time.Second)))
			Expect(errTimer).NotTo(HaveOccurred())
			Eventually(func() (int64, error) {
				v, err := consumerTimer.QueryOffset()
				// we can ignore the offset not found error here
				if err != nil {
					return 0, nil
				}
				return v, err
			}, 5*time.Second).WithPolling(500*time.Millisecond).Should(Equal(int64(104)),
				"Offset should be 104")
			Expect(consumerTimer.Close()).NotTo(HaveOccurred())
		})

	})

	It("commit at flush interval with constant stream of incoming messages", func() {
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		var messagesReceived int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(_ ConsumerContext, _ *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)
			}, NewConsumerOptions().
				SetConsumerName("autoCommitStrategy").
				SetAutoCommit(NewAutoCommitStrategy().
					SetCountBeforeStorage(10000000).
					SetFlushInterval(time.Second)))
		Expect(err).NotTo(HaveOccurred())

		maxMessages := 50
		for i := 0; i < maxMessages; i++ {
			Expect(producer.Send(CreateMessageForTesting("", i))).NotTo(HaveOccurred())
			// emit message before the flush interval has elapsed
			time.Sleep(time.Millisecond * 1100)

			v, err := consumer.QueryOffset()
			Expect(err).NotTo(HaveOccurred())
			if v > 0 {
				break
			}

		}

		Expect(messagesReceived > 0 && messagesReceived < int32(maxMessages)).To(BeTrueBecause("%d messages received", messagesReceived))
		Expect(producer.Close()).NotTo(HaveOccurred())
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("Deduplication", func() {
		producerName := "producer-ded"
		producer, err := env.NewProducer(streamName, NewProducerOptions().SetProducerName(producerName))
		Expect(err).NotTo(HaveOccurred())
		var arr []message.StreamMessage
		for z := 0; z < 10; z++ {
			m := amqp.NewMessage([]byte("test_" + strconv.Itoa(z)))
			m.SetPublishingId(int64(z * 10)) // id stored: the last one should be the same on QuerySequence
			arr = append(arr, m)
		}

		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm, p *Producer) {
			defer GinkgoRecover()
			for ids := range ch {
				for _, msg := range ids {
					Expect(msg.GetError()).NotTo(HaveOccurred())
					Expect(msg.GetProducerID()).To(Equal(p.id))
					Expect(msg.GetMessage().HasPublishingId()).To(Equal(true))
					Expect(msg.IsConfirmed()).To(Equal(true))
					Expect(msg.message.GetPublishingId()).To(Equal(msg.GetPublishingId()))
				}
			}
		}(chConfirm, producer)

		// here we handle the deduplication, so we must have only
		// 10 messages on the stream, since we are using the
		// same SetPublishingId
		// even we publish the same array more times
		for i := 0; i < 10; i++ {
			err = producer.BatchSend(arr)
			Expect(err).NotTo(HaveOccurred())

		}

		var messagesReceived int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(_ ConsumerContext, _ *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(10)),
			"consumer should receive only 10 messages")

		Eventually(func() int64 {
			v, _ := env.QuerySequence(producerName, streamName)
			return v
		}, 5*time.Second).Should(Equal(int64(90)),
			"QuerySequence should give the last id: 90")

		Eventually(func() int64 {
			v, _ := producer.GetLastPublishingId()
			return v
		}, 5*time.Second).Should(Equal(int64(90)),
			"GetLastPublishingId should give the last id: 90")

		Expect(producer.Close()).NotTo(HaveOccurred())
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("last consumed message not raise an error fist time", func() {

		_, err := env.NewConsumer(streamName,
			func(_ ConsumerContext, _ *amqp.Message) {
			}, NewConsumerOptions().
				SetOffset(OffsetSpecification{}.LastConsumed()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
	})

	It("Subscribe/Unsubscribe count messages manual store", func() {
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		// the offset doesn't exist (yet) here for the consumer test
		_, err = env.QueryOffset("consumer_test", streamName)
		Expect(err).To(HaveOccurred())

		err = producer.BatchSend(CreateArrayMessagesForTesting(107))
		Expect(err).NotTo(HaveOccurred())

		Expect(producer.Close()).NotTo(HaveOccurred())
		var messagesReceived int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, _ *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)
				_ = consumerContext.Consumer.StoreOffset()
			}, NewConsumerOptions().
				SetOffset(OffsetSpecification{}.First()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(107)),
			"consumer should receive same messages Send by producer")

		Eventually(func() (int64, error) {
			return consumer.QueryOffset()
			// 106 is the offset since it starts from 0
		}, 5*time.Second).Should(Equal(int64(106)),
			"Offset should be 106")
		time.Sleep(500 * time.Millisecond)
		offset, err := env.QueryOffset("consumer_test", streamName)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() int64 {
			return offset
			// 106 is the offset since it starts from 0
		}, 5*time.Second).Should(Equal(int64(106)),
			"Offset should be 106")

		offsetConsumer, err := consumer.QueryOffset()
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() int64 {
			return offsetConsumer
			// 106 is the offset since it starts from 0
		}, 5*time.Second).Should(Equal(int64(106)),
			"Consumer Offset should be 106")

		err = consumer.Close()
		Expect(err).NotTo(HaveOccurred())

		atomic.SwapInt32(&messagesReceived, 0)
		consumer, err = env.NewConsumer(streamName,
			func(_ ConsumerContext, _ *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)
			}, NewConsumerOptions().
				SetOffset(OffsetSpecification{}.LastConsumed()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesReceived)).To(Equal(int32(1)))
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("Check already closed", func() {
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		err = producer.BatchSend(CreateArrayMessagesForTesting(1))
		Expect(err).NotTo(HaveOccurred())

		defer func(producer *Producer) {
			err := producer.Close()
			Expect(err).NotTo(HaveOccurred())
		}(producer)

		var messagesCount int32 = 0
		var signal = make(chan struct{})
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, _ *amqp.Message) {
				if atomic.AddInt32(&messagesCount, 1) >= 1 {
					time.Sleep(500 * time.Millisecond)
					err1 := consumerContext.Consumer.Close()
					Expect(err1).NotTo(HaveOccurred())
					signal <- struct{}{}
				}
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())

		<-signal
		time.Sleep(200 * time.Millisecond)
		Expect(consumer.Close()).To(Equal(AlreadyClosed))

	})

	It("Not panics on close when the internal queue is full", func() {
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		err = producer.BatchSend(CreateArrayMessagesForTesting(10_000))
		Expect(err).NotTo(HaveOccurred())

		defer func(producer *Producer) {
			err := producer.Close()
			Expect(err).NotTo(HaveOccurred())
		}(producer)

		const credits = 2
		consumer, err := env.NewConsumer(streamName,
			func(_ ConsumerContext, _ *amqp.Message) {
				// it usually happens with slow consumer that fulfill the internal queue
				time.Sleep(time.Hour)
			}, NewConsumerOptions().
				SetInitialCredits(credits).
				SetOffset(OffsetSpecification{}.First()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())

		// waiting the internal queue to fulfill
		time.Sleep(time.Second)
		// - 1 because the first message was consumed by the handler
		Expect(len(consumer.chunkForConsumer)).To(Equal(credits - 1))

		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("message Properties", func() {
		producer, err := env.NewProducer(streamName, nil)

		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm, _ *Producer) {
			defer GinkgoRecover()
			for ids := range ch {
				for _, msg := range ids {
					Expect(msg.GetMessage().GetMessageProperties().To).To(Equal("ToTest"))
					Expect(msg.GetMessage().GetMessageProperties().Subject).To(Equal("SubjectTest"))
					Expect(msg.GetMessage().GetMessageProperties().ReplyTo).To(Equal("replyToTest"))
					Expect(msg.GetMessage().GetMessageProperties().ContentType).To(Equal("ContentTypeTest"))
					Expect(msg.GetMessage().GetMessageProperties().ContentEncoding).To(Equal("ContentEncodingTest"))
				}
			}
		}(chConfirm, producer)
		Expect(err).NotTo(HaveOccurred())
		msg := amqp.NewMessage([]byte{0x00, 0x0e, 0x01, 0x0f, 0x05, 0x08, 0x04, 0x03})
		msg.Properties = &amqp.MessageProperties{
			MessageID:          nil,
			UserID:             nil,
			To:                 "ToTest",
			Subject:            "SubjectTest",
			ReplyTo:            "replyToTest",
			CorrelationID:      nil,
			ContentType:        "ContentTypeTest",
			ContentEncoding:    "ContentEncodingTest",
			AbsoluteExpiryTime: time.Time{},
			CreationTime:       time.Time{},
			GroupID:            "",
			GroupSequence:      0,
			ReplyToGroupID:     "",
		}

		Expect(producer.Send(msg)).NotTo(HaveOccurred())
		defer func(producer *Producer) {
			Expect(producer.Close()).NotTo(HaveOccurred())
		}(producer)

		consumer, err := env.NewConsumer(streamName,
			func(_ ConsumerContext, message *amqp.Message) {
				Expect(message.Properties.ReplyTo).To(Equal("replyToTest"))
				Expect(message.Properties.Subject).To(Equal("SubjectTest"))
				Expect(message.Properties.To).To(Equal("ToTest"))
				Expect(message.Properties.ContentType).To(Equal("ContentTypeTest"))
				Expect(message.Properties.ContentEncoding).To(Equal("ContentEncodingTest"))
				Expect(message.Data[0]).To(Equal([]byte{0x00, 0x0e, 0x01, 0x0f, 0x05, 0x08, 0x04, 0x03}))

			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		Expect(consumer.Close()).NotTo(HaveOccurred())

	})

	It("Application Message Properties", func() {
		producer, err := env.NewProducer(streamName, nil)

		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm, _ *Producer) {
			defer GinkgoRecover()
			for ids := range ch {
				for _, msg := range ids {
					Expect(msg.GetMessage().GetApplicationProperties()["key1"]).To(Equal("value1"))
					Expect(msg.GetMessage().GetApplicationProperties()["key2"]).To(Equal("value2"))
					Expect(msg.GetMessage().GetApplicationProperties()["key3"]).To(Equal("value3"))
					Expect(msg.GetMessage().GetApplicationProperties()["key4"]).To(Equal("value4"))
					Expect(msg.GetMessage().GetMessageAnnotations()["annotation_key_1"]).To(Equal("annotation_vale_1"))
					Expect(msg.GetMessage().GetMessageAnnotations()["annotation_key_2"]).To(Equal("annotation_vale_2"))
					Expect(msg.GetMessage().GetMessageHeader()).To(BeNil())
					Expect(msg.GetMessage().GetAMQPValue()).To(BeNil())
				}
			}
		}(chConfirm, producer)

		appMap := map[string]any{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
			"key4": "value4",
			"key5": "value5",
		}
		Expect(err).NotTo(HaveOccurred())
		msg := amqp.NewMessage([]byte("message"))
		msg.ApplicationProperties = appMap
		msg.Annotations = map[any]any{
			"annotation_key_1": "annotation_vale_1",
			"annotation_key_2": "annotation_vale_2",
		}

		Expect(producer.Send(msg)).NotTo(HaveOccurred())
		defer func(producer *Producer) {
			Expect(producer.Close()).NotTo(HaveOccurred())
		}(producer)

		consumer, err := env.NewConsumer(streamName,
			func(_ ConsumerContext, message *amqp.Message) {
				Expect(message.ApplicationProperties["key1"]).To(Equal("value1"))
				Expect(message.ApplicationProperties["key2"]).To(Equal("value2"))
				Expect(message.ApplicationProperties["key3"]).To(Equal("value3"))
				Expect(message.ApplicationProperties["key4"]).To(Equal("value4"))
				Expect(message.Annotations["annotation_key_1"]).To(Equal("annotation_vale_1"))
				Expect(message.Annotations["annotation_key_2"]).To(Equal("annotation_vale_2"))

			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		Expect(consumer.Close()).NotTo(HaveOccurred())

	})

	It("Consistent Messages", func() {
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		for z := 0; z < 2034; z++ {
			err := producer.Send(amqp.NewMessage([]byte("test_" + strconv.Itoa(z))))
			Expect(err).NotTo(HaveOccurred())
		}
		time.Sleep(200 * time.Millisecond)
		Expect(producer.Close()).NotTo(HaveOccurred())
		mt := &sync.Mutex{}
		var messagesValue []string
		consumer, err := env.NewConsumer(streamName,
			func(_ ConsumerContext, message *amqp.Message) {
				mt.Lock()
				defer mt.Unlock()
				messagesValue = append(messagesValue, string(message.Data[0]))

			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())

		mt.Lock()
		for i := range messagesValue {
			Expect(messagesValue[i]).To(Equal("test_" + strconv.Itoa(i)))
		}
		mt.Unlock()
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("Should not skip chunks on slow consumer", func() {
		// see: https://github.com/rabbitmq/rabbitmq-stream-go-client/pull/412
		const credits = 2
		const numMessages = 10_000
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		err = producer.BatchSend(CreateArrayMessagesForTesting(numMessages))
		Expect(err).NotTo(HaveOccurred())

		defer func(producer *Producer) {
			err := producer.Close()
			Expect(err).NotTo(HaveOccurred())
		}(producer)

		lastOffset := atomic.Int64{}
		lastOffset.Store(-1)
		consumer, err := env.NewConsumer(streamName,
			func(ctx ConsumerContext, _ *amqp.Message) {
				offset := ctx.Consumer.GetOffset()
				Expect(offset).To(Equal(lastOffset.Load() + 1))
				lastOffset.Store(offset)

				if offset%1_000 == 0 {
					// simulating an intensive operation
					time.Sleep(500 * time.Millisecond)
				}
			}, NewConsumerOptions().
				SetInitialCredits(credits).
				SetOffset(OffsetSpecification{}.First()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())

		// it subtract 1 because the offset starts at 0
		//nolint:gocritic
		Eventually(func() int64 { return lastOffset.Load() }, 10*time.Second).
			Should(
				Equal(int64(numMessages-1)),
				"not all the messages has been consumed %d out of %d",
				lastOffset.Load(),
				numMessages-1,
			)

		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("Validation", func() {
		_, err := env.NewConsumer(streamName,
			func(_ ConsumerContext, _ *amqp.Message) {
			}, &ConsumerOptions{
				Offset: OffsetSpecification{},
			})
		Expect(err).To(HaveOccurred())

		_, err = env.NewConsumer(streamName,
			func(_ ConsumerContext, _ *amqp.Message) {
			}, NewConsumerOptions().SetAutoCommit(
				NewAutoCommitStrategy().
					SetCountBeforeStorage(-1)))
		Expect(err).To(HaveOccurred())

		_, err = env.NewConsumer(streamName,
			func(_ ConsumerContext, _ *amqp.Message) {
			}, NewConsumerOptions().SetAutoCommit(
				NewAutoCommitStrategy().SetFlushInterval(10*time.Millisecond)))
		Expect(err).To(HaveOccurred())

		// message specific a valid offset
		_, err = env.NewConsumer(streamName,
			nil, &ConsumerOptions{
				Offset: OffsetSpecification{},
			})
		Expect(err).To(HaveOccurred())

		// handler is nil
		_, err = env.NewConsumer(streamName,
			nil, &ConsumerOptions{
				Offset: OffsetSpecification{
					typeOfs: typeFirst},
			})
		Expect(err).To(HaveOccurred())

		_, err = env.NewConsumer(streamName,
			nil, NewConsumerOptions().SetAutoCommit(NewAutoCommitStrategy()))
		Expect(err).To(HaveOccurred())

	})

	It("Sub Batch consumer with different publishers GZIP and Not", func() {
		producer1, err := env.NewProducer(streamName,
			NewProducerOptions().SetBatchPublishingDelay(100).
				SetSubEntrySize(35))
		Expect(err).NotTo(HaveOccurred())
		producer2, err := env.NewProducer(streamName,
			NewProducerOptions().SetBatchPublishingDelay(50).
				SetSubEntrySize(1055).SetCompression(Compression{}.Zstd()))
		Expect(err).NotTo(HaveOccurred())
		producer3, err := env.NewProducer(streamName,
			NewProducerOptions().SetBatchPublishingDelay(50))
		Expect(err).NotTo(HaveOccurred())

		producer4, err := env.NewProducer(streamName,
			NewProducerOptions().SetBatchPublishingDelay(50).
				SetSubEntrySize(2).
				SetCompression(Compression{}.Lz4()))
		Expect(err).NotTo(HaveOccurred())

		producer5, err := env.NewProducer(streamName,
			NewProducerOptions().SetBatchPublishingDelay(500).
				SetSubEntrySize(56).SetCompression(Compression{}.Snappy()))
		Expect(err).NotTo(HaveOccurred())

		producer6Batch, err := env.NewProducer(streamName,
			NewProducerOptions().SetBatchPublishingDelay(500).
				SetSubEntrySize(56).SetCompression(Compression{}.Gzip()))
		Expect(err).NotTo(HaveOccurred())

		var batchMessages []message.StreamMessage
		for i := 0; i < 20; i++ {
			v := ""
			min := 2
			max := 100
			for i := 0; i < rand.Intn(max-min)+min; i++ {
				v += "T"
			}
			msg := amqp.NewMessage([]byte(v))
			batchMessages = append(batchMessages, msg)
		}
		totalMessages := 200
		for i := 0; i < totalMessages; i++ {

			Expect(producer1.Send(amqp.NewMessage(make([]byte, 50)))).NotTo(HaveOccurred())
			Expect(producer2.Send(amqp.NewMessage(make([]byte, 50)))).NotTo(HaveOccurred())
			Expect(producer3.Send(amqp.NewMessage(make([]byte, 50)))).NotTo(HaveOccurred())
			Expect(producer4.Send(amqp.NewMessage(make([]byte, 50)))).NotTo(HaveOccurred())
			Expect(producer5.Send(amqp.NewMessage(make([]byte, 50)))).NotTo(HaveOccurred())
		}

		for i := 0; i < 50; i++ {
			err2 := producer6Batch.BatchSend(batchMessages)
			Expect(err2).NotTo(HaveOccurred())
		}

		var messagesReceived int32
		consumer, err := env.NewConsumer(streamName,
			func(_ ConsumerContext, _ *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)

			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 15*time.Second).Should(Equal(int32((totalMessages*5)+(50*len(batchMessages)))),
			"consumer should be the same sent from different publishers settings")

		Expect(producer1.Close()).NotTo(HaveOccurred())
		Expect(producer2.Close()).NotTo(HaveOccurred())
		Expect(producer3.Close()).NotTo(HaveOccurred())
		Expect(producer4.Close()).NotTo(HaveOccurred())
		Expect(producer5.Close()).NotTo(HaveOccurred())
		Expect(producer6Batch.Close()).NotTo(HaveOccurred())
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("Deduplication in Sub Batch", func() {
		producer, err := env.NewProducer(streamName,
			NewProducerOptions().
				SetProducerName("producer-ded-sub").
				SetSubEntrySize(3).
				SetCompression(Compression{}.Gzip()))
		Expect(err).NotTo(HaveOccurred())
		var arr []message.StreamMessage
		for z := 0; z < 10; z++ {
			m := amqp.NewMessage([]byte("test_" + strconv.Itoa(z)))
			m.SetPublishingId(int64(z * 10))
			arr = append(arr, m)
		}

		// deduplication is disabled on sub-batching
		// so, even we set the SetPublishingId
		// it will be ignored
		for i := 0; i < 10; i++ {
			err := producer.BatchSend(arr)
			Expect(err).NotTo(HaveOccurred())
		}

		var messagesReceived int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(_ ConsumerContext, _ *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(10*10)),
			"consumer should receive same messages Send by producer")
		Expect(producer.Close()).NotTo(HaveOccurred())
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("Manual Credit Strategy", func() {
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		const batchSize = 500
		err = producer.BatchSend(CreateArrayMessagesForTesting(batchSize))
		Expect(err).NotTo(HaveOccurred())

		msgCh := make(chan *amqp.Message)
		consumer, err := env.NewConsumer(streamName,
			func(_ ConsumerContext, msg *amqp.Message) {
				msgCh <- msg
			}, NewConsumerOptions().
				SetOffset(OffsetSpecification{}.First()).
				SetCreditStrategy(ManualCreditStrategy).
				SetInitialCredits(1))
		Expect(err).NotTo(HaveOccurred())

		// Sad workaround to avoid asserting too soon when there's nothing in the channel
		<-time.After(time.Millisecond * 100)

		// Eventually, it should exhaust the credits
		Eventually(msgCh).Within(3*time.Second).ShouldNot(Receive(), "expected no messages after exhausting credits")
		// Give more credits to consume the entire batch of 500 messages
		Expect(consumer.Credit(20)).To(Succeed())
		// Eventually, it should receive the last message
		Eventually(msgCh).Within(10 * time.Second).Should(Receive(test_helper.HaveMatchingData("test_499")))
		// It should not receive any more messages, because the entire batch of 500 messages has been consumed
		Consistently(msgCh).ShouldNot(Receive())

		Expect(producer.Close()).NotTo(HaveOccurred())
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

})
