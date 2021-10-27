package stream

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
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
		for i := 0; i < 10; i++ {
			consumer, err := env.NewConsumer(streamName,
				func(consumerContext ConsumerContext, message *amqp.Message) {

				}, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(consumer.ID).To(Equal(uint8(0)))
			consumers = append(consumers, consumer)
		}

		Expect(len(env.consumers.getCoordinators())).To(Equal(1))
		Expect(len(env.consumers.getCoordinators()["localhost:5552"].
			getClientsPerContext())).To(Equal(10))

		for _, consumer := range consumers {
			Expect(consumer.Close()).NotTo(HaveOccurred())
		}

		Expect(len(env.consumers.getCoordinators()["localhost:5552"].
			getClientsPerContext())).To(Equal(0))

	})

	It("Multi Consumers per client", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().SetMaxConsumersPerClient(2))
		Expect(err).NotTo(HaveOccurred())
		streamName := uuid.New().String()
		Expect(env.DeclareStream(streamName, nil)).
			NotTo(HaveOccurred())

		for i := 0; i < 10; i++ {
			consumer, err := env.NewConsumer(streamName,
				func(consumerContext ConsumerContext, message *amqp.Message) {

				}, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(consumer.ID).To(Equal(uint8(i % 2)))
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
			func(consumerContext ConsumerContext, message *amqp.Message) {

			}, nil)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(10 * time.Millisecond)
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("Subscribe fail not exist", func() {
		_, err := env.NewConsumer("NOT_EXIST",
			func(consumerContext ConsumerContext, message *amqp.Message) {

			}, nil)

		Expect(errors.Cause(err)).To(Equal(StreamDoesNotExist))
		Expect(env.Close()).NotTo(HaveOccurred())
	})

	It("Consumer close handler unSubscribe", func() {
		var commandIdRecv int32

		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
			}, nil)
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
			"command received should be CommandMetadataUpdate ")

		Expect(err).NotTo(HaveOccurred())
	})

	It("Consumer close handler meta data", func() {
		var commandIdRecv int32
		streamName := uuid.New().String()
		Expect(env.DeclareStream(streamName, nil)).NotTo(HaveOccurred())
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
			}, nil)
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

		err = producer.BatchSend(CreateArrayMessagesForTesting(100)) // batch send
		Expect(err).NotTo(HaveOccurred())
		// we can't close the subscribe until the publish is finished
		time.Sleep(200 * time.Millisecond)
		Expect(producer.Close()).NotTo(HaveOccurred())
		var messagesReceived int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.Offset(50)))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(50)),
			"consumer should only 50 messages due the offset 50")

		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("Deduplication", func() {
		producer, err := env.NewProducer(streamName, NewProducerOptions().SetProducerName("producer-ded"))
		Expect(err).NotTo(HaveOccurred())
		var arr []message.StreamMessage
		for z := 0; z < 10; z++ {
			m := amqp.NewMessage([]byte("test_" + strconv.Itoa(z)))
			m.SetPublishingId(int64(z * 10))
			arr = append(arr, m)
		}

		chConfirm := producer.NotifyPublishConfirmation()
		go func(ch ChannelPublishConfirm, p *Producer) {
			for ids := range ch {
				for _, msg := range ids {
					Expect(msg.GetError()).NotTo(HaveOccurred())
					Expect(msg.GetProducerID()).To(Equal(p.id))
					Expect(msg.GetMessage().HasPublishingId()).To(Equal(true))
					Expect(msg.IsConfirmed()).To(Equal(true))
					Expect(msg.message.GetPublishingId()).To(Equal(msg.GetPublishingIdAssigned()))
				}
			}
		}(chConfirm, producer)

		// here we handle the deduplication, so we must have only
		// 10 messages on the stream, since we are using the
		// same SetPublishingId
		// even we publish the same array more times
		for i := 0; i < 10; i++ {
			Expect(producer.BatchSend(arr)).NotTo(HaveOccurred())
		}

		var messagesReceived int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(10)),
			"consumer should receive only 10 messages")
		Expect(producer.Close()).NotTo(HaveOccurred())
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("Subscribe/Unsubscribe count messages", func() {
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		Expect(producer.BatchSend(CreateArrayMessagesForTesting(107))).
			NotTo(HaveOccurred())
		// we can't close the subscribe until the publish is finished
		time.Sleep(500 * time.Millisecond)
		Expect(producer.Close()).NotTo(HaveOccurred())
		var messagesReceived int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)
				_ = consumerContext.Consumer.StoreOffset()
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(107)),
			"consumer should receive same messages send by producer")

		err = consumer.Close()
		Expect(err).NotTo(HaveOccurred())

		atomic.SwapInt32(&messagesReceived, 0)
		consumer, err = env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.LastConsumed()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
		Expect(atomic.LoadInt32(&messagesReceived)).To(Equal(int32(0)))
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

	It("Check already closed", func() {
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(producer.BatchSend(CreateArrayMessagesForTesting(500))).
			NotTo(HaveOccurred())
		defer func(producer *Producer) {
			err := producer.Close()
			Expect(err).NotTo(HaveOccurred())
		}(producer)

		var messagesCount int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				if atomic.AddInt32(&messagesCount, 1) >= 1 {
					err := consumerContext.Consumer.Close()
					if err != nil {
						return
					}
				}
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(200 * time.Millisecond)
		Expect(consumer.Close()).To(Equal(AlreadyClosed))

	})

	It("message Properties", func() {
		producer, err := env.NewProducer(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		msg := amqp.NewMessage([]byte("message"))
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
			func(consumerContext ConsumerContext, message *amqp.Message) {
				Expect(message.Properties.ReplyTo).To(Equal("replyToTest"))
				Expect(message.Properties.Subject).To(Equal("SubjectTest"))
				Expect(message.Properties.To).To(Equal("ToTest"))
				Expect(message.Properties.ContentType).To(Equal("ContentTypeTest"))
				Expect(message.Properties.ContentEncoding).To(Equal("ContentEncodingTest"))

			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).SetConsumerName("consumer_test"))
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
			func(consumerContext ConsumerContext, message *amqp.Message) {
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

	It("Validation", func() {
		_, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
			}, &ConsumerOptions{
				Offset: OffsetSpecification{},
			})
		Expect(err).To(HaveOccurred())
	})

	It("Sub Batch consumer with different publishers GZIP and Not", func() {
		producer1, err := env.NewProducer(streamName,
			NewProducerOptions().SetBatchPublishingDelay(100).
				SetSubEntrySize(35))
		Expect(err).NotTo(HaveOccurred())
		producer2, err := env.NewProducer(streamName,
			NewProducerOptions().SetBatchPublishingDelay(50).
				SetSubEntrySize(1055))
		Expect(err).NotTo(HaveOccurred())
		producer3, err := env.NewProducer(streamName,
			NewProducerOptions().SetBatchPublishingDelay(50))
		Expect(err).NotTo(HaveOccurred())

		producer4, err := env.NewProducer(streamName,
			NewProducerOptions().SetBatchPublishingDelay(50).
				SetSubEntrySize(2).
				SetCompression(Compression{}.Gzip()))
		Expect(err).NotTo(HaveOccurred())

		producer5, err := env.NewProducer(streamName,
			NewProducerOptions().SetBatchPublishingDelay(500).
				SetSubEntrySize(56).SetCompression(Compression{}.Gzip()))
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

		for i := 0; i < 2000; i++ {
			msg := amqp.NewMessage(make([]byte, 50))
			Expect(producer1.Send(msg)).NotTo(HaveOccurred())
			Expect(producer2.Send(msg)).NotTo(HaveOccurred())
			Expect(producer3.Send(msg)).NotTo(HaveOccurred())
			Expect(producer4.Send(msg)).NotTo(HaveOccurred())
			Expect(producer5.Send(msg)).NotTo(HaveOccurred())
		}

		for i := 0; i < 50; i++ {
			Expect(producer6Batch.BatchSend(batchMessages)).NotTo(HaveOccurred())
		}

		var messagesReceived int32
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)

			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()).
				SetConsumerName("consumer_test"))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 9*time.Second).Should(Equal(int32((2000*5)+(50*len(batchMessages)))),
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
			Expect(producer.BatchSend(arr)).NotTo(HaveOccurred())
		}

		var messagesReceived int32 = 0
		consumer, err := env.NewConsumer(streamName,
			func(consumerContext ConsumerContext, message *amqp.Message) {
				atomic.AddInt32(&messagesReceived, 1)
			}, NewConsumerOptions().SetOffset(OffsetSpecification{}.First()))
		Expect(err).NotTo(HaveOccurred())

		Eventually(func() int32 {
			return atomic.LoadInt32(&messagesReceived)
		}, 5*time.Second).Should(Equal(int32(10*10)),
			"consumer should receive same messages send by producer")
		Expect(producer.Close()).NotTo(HaveOccurred())
		Expect(consumer.Close()).NotTo(HaveOccurred())
	})

})
