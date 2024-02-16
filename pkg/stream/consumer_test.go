package stream

import (
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/constants"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"go.uber.org/mock/gomock"
	"reflect"
	"sync"
)

var _ = Describe("Smart Consumer", func() {
	// Create a consumer
	// Validate params
	// Consumes Message

	// Mock the raw layer with gomock
	// Call raw loyer Subscribe() to create a new consumer.
	// NotifyChunk()
	// Unsubscribe to close the consumer
	var (
		mockController *gomock.Controller
		fakeRawClient  *MockRawClient
		ctxType        = reflect.TypeOf((*context.Context)(nil)).Elem()
		dataBuf        *gbytes.Buffer
	)

	BeforeEach(func() {
		mockController = gomock.NewController(GinkgoT())
		fakeRawClient = NewMockRawClient(mockController)
		dataBuf = gbytes.NewBuffer()
	})

	Context("validating parameters", func() {
		opts := &ConsumerOptions{}
		handleMessages := func(consumerContext ConsumerContext, message *amqp.Message) {
			fmt.Printf("messages: %s\n", message.Data)
		}
		It("stream name", func() {
			_, err := NewConsumer("", fakeRawClient, handleMessages, opts, &sync.Mutex{})
			Expect(err).To(MatchError("stream name must not be empty"))
		})

		It("uses sensible defaults for options if unset", func() {
			consumer, err := NewConsumer("stream", fakeRawClient, handleMessages, opts, &sync.Mutex{})
			Expect(err).NotTo(HaveOccurred())

			Expect(consumer.opts.OffsetType).To(Equal(constants.OffsetTypeFirst))
		})
	})

	It("consume messages from the first offset", func() {
		//setup
		chunkChan := make(chan *raw.Chunk)
		gomock.InOrder(fakeRawClient.EXPECT().
			NotifyChunk(gomock.AssignableToTypeOf(chunkChan)).Return(chunkChan),
			fakeRawClient.EXPECT().
				Subscribe(
					gomock.AssignableToTypeOf(ctxType),                   //context
					gomock.Eq("test-stream"),                             // stream
					gomock.Eq(uint16(1)),                                 // offsetType
					gomock.Eq(uint8(1)),                                  // subscriptionId
					gomock.Eq(uint16(2)),                                 // credit
					gomock.AssignableToTypeOf(raw.SubscribeProperties{}), // properties
					gomock.Eq(uint64(1)),                                 // offset
				))
		// create amqp message and convert to bytes
		s := "rabbit"
		bs := []byte(s)
		fakeMsg := amqp.Message{Data: bs}
		fakeMsgBytes, err := fakeMsg.MarshalBinary()
		Expect(err).NotTo(HaveOccurred())

		fakeChunk := &raw.Chunk{Messages: fakeMsgBytes}

		opts := &ConsumerOptions{
			OffsetType:     uint16(1),
			SubscriptionId: uint8(1),
			InitialCredits: uint16(2),
			Offset:         uint64(1),
		}
		handleMessages := func(consumerContext ConsumerContext, message *amqp.Message) {
			_, err := dataBuf.Write(message.Data)
			if err != nil {
				fmt.Println("error writing message to gbytes buffer", err)
			}
		}
		consumer, _ := NewConsumer("test-stream", fakeRawClient, handleMessages, opts, &sync.Mutex{})
		consumer.chunkCh = chunkChan
		chunkChan <- fakeChunk
		Eventually(dataBuf).Should(gbytes.Say("rabbit"))
	})

	It("stores the current and last known offset", func() {
		//setup
		handleMessages := func(consumerContext ConsumerContext, message *amqp.Message) {
			fmt.Printf("messages: %s\n", message.Data)
		}
		opts := &ConsumerOptions{
			OffsetType:     uint16(1),
			SubscriptionId: uint8(1),
			InitialCredits: uint16(2),
			Offset:         uint64(1),
		}
		consumer, _ := NewConsumer("test-stream", fakeRawClient, handleMessages, opts, &sync.Mutex{})
		consumer.setCurrentOffset(uint64(666))
		Expect(consumer.GetOffset()).To(BeNumerically("==", 666))
		consumer.updateLastStoredOffset()
		consumer.setCurrentOffset(uint64(667))
		Expect(consumer.GetOffset()).To(BeNumerically("==", 667))
		Expect(consumer.GetLastStoredOffset()).To(BeNumerically("==", 666))
	})

	It("unsubscribes from the stream", func() {
		//setup
		handleMessages := func(consumerContext ConsumerContext, message *amqp.Message) {
			fmt.Printf("messages: %s\n", message.Data)
		}
		fakeRawClient.EXPECT().
			Unsubscribe(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf(uint8(1)))

		opts := &ConsumerOptions{
			OffsetType:     uint16(1),
			SubscriptionId: uint8(1),
			InitialCredits: uint16(2),
			Offset:         uint64(1),
		}
		//test
		consumer, _ := NewConsumer("test-stream", fakeRawClient, handleMessages, opts, &sync.Mutex{})
		Expect(consumer).ToNot(BeNil())
		Expect(consumer.Close(context.Background())).To(Succeed())

	})

	It("subscribes with initial credits", func() {
		//setup
		handleMessages := func(consumerContext ConsumerContext, message *amqp.Message) {
			fmt.Printf("messages: %s\n", message.Data)
		}
		chunkChan := make(chan *raw.Chunk)
		gomock.InOrder(fakeRawClient.EXPECT().
			NotifyChunk(gomock.AssignableToTypeOf(chunkChan)).Return(chunkChan),
			fakeRawClient.EXPECT().
				Subscribe(
					gomock.AssignableToTypeOf(ctxType),                   //context
					gomock.Eq("test-stream"),                             // stream
					gomock.Eq(uint16(1)),                                 // offsetType
					gomock.Eq(uint8(1)),                                  // subscriptionId
					gomock.Eq(uint16(500)),                               // credit
					gomock.AssignableToTypeOf(raw.SubscribeProperties{}), // properties
					gomock.Eq(uint64(1)),                                 // offset
				))

		opts1 := &ConsumerOptions{
			OffsetType:     uint16(1),
			SubscriptionId: uint8(1),
			Offset:         uint64(1),
			InitialCredits: uint16(500),
		}
		opts2 := &ConsumerOptions{
			OffsetType:     uint16(1),
			SubscriptionId: uint8(1),
			Offset:         uint64(1),
			InitialCredits: uint16(1001),
		}

		//test
		_, err := NewConsumer("test-stream", fakeRawClient, handleMessages, opts1, &sync.Mutex{})
		Expect(err).NotTo(HaveOccurred())

		_, err = NewConsumer("test-stream", fakeRawClient, handleMessages, opts2, &sync.Mutex{})
		Expect(err).To(MatchError("initial credits cannot be greater than 1000"))
	})

	Describe("Single Active Consumer", func() {
		It("can enable the feature", func() {
			subscribeProperties := raw.SubscribeProperties{"single-active-consumer": "true"}
			chunkChan := make(chan *raw.Chunk)
			gomock.InOrder(fakeRawClient.EXPECT().
				NotifyChunk(gomock.AssignableToTypeOf(chunkChan)).Return(chunkChan),
				fakeRawClient.EXPECT().
					Subscribe(
						gomock.AssignableToTypeOf(ctxType), //context
						gomock.Eq("test-stream"),           // stream
						gomock.Eq(uint16(1)),               // offsetType
						gomock.Eq(uint8(1)),                // subscriptionId
						gomock.Eq(uint16(2)),               // credit
						gomock.Eq(subscribeProperties),     // properties
						gomock.Eq(uint64(1)),               // offset
					))

			opts := &ConsumerOptions{
				SingleActiveConsumer: true,
				OffsetType:           uint16(1),
				SubscriptionId:       uint8(1),
				InitialCredits:       uint16(2),
				Offset:               uint64(1),
			}
			handleMessages := func(consumerContext ConsumerContext, message *amqp.Message) {
				fmt.Printf("messages: %s\n", message.Data)
			}
			consumer, _ := NewConsumer("test-stream", fakeRawClient, handleMessages, opts, &sync.Mutex{})
			Expect(consumer.getStatus()).To(Equal(0))
			consumer.setStatus(1)
			Expect(consumer.getStatus()).To(Equal(1))
		})
	})

	Describe("SuperStream Consumer", func() {
		It("can enable the feature", func() {
		})
	})
})
