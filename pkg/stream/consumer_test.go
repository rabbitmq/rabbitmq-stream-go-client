package stream

import (
	"context"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/constants"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"go.uber.org/mock/gomock"
	"reflect"
	"sync/atomic"
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
	)

	BeforeEach(func() {
		mockController = gomock.NewController(GinkgoT())
		fakeRawClient = NewMockRawClient(mockController)
	})

	Context("validating parameters", func() {
		opts := &ConsumerOptions{}
		var count int32
		handleMessages := func(ctxType context.Context, message *amqp.Message) {
			if atomic.AddInt32(&count, 1)%1000 == 0 {
				fmt.Printf("cousumed %d  messages \n", atomic.LoadInt32(&count))
				// AVOID to store for each single message, it will reduce the performances
				// The server keeps the consume tracking using the consumer name
			}
		}
		It("stream name", func() {
			_, err := NewConsumer("", fakeRawClient, handleMessages, opts)
			Expect(err).To(MatchError("stream name must not be empty"))
		})

		It("uses sensible defaults for options if unset", func() {
			consumer, err := NewConsumer("stream", fakeRawClient, handleMessages, opts)
			Expect(err).NotTo(HaveOccurred())

			Expect(consumer.opts.OffsetType).To(Equal(constants.OffsetTypeFirst))
		})
	})

	It("consume messages from the first offset", func() {
		//setup
		var count int32
		chunkChan := make(chan *raw.Chunk)
		handleMessages := func(ctxType context.Context, message *amqp.Message) {
			if atomic.AddInt32(&count, 1)%1000 == 0 {
				fmt.Printf("cousumed %d  messages \n", atomic.LoadInt32(&count))
				// AVOID to store for each single message, it will reduce the performances
				// The server keeps the consume tracking using the consumer name
			}
		}
		gomock.InOrder(fakeRawClient.EXPECT().
			Subscribe(
				gomock.AssignableToTypeOf(ctxType),                   //context
				gomock.Eq("test-stream"),                             // stream
				gomock.Eq(uint16(1)),                                 // offsetType
				gomock.Eq(uint8(1)),                                  // subscriptionId
				gomock.Eq(uint16(2)),                                 // credit
				gomock.AssignableToTypeOf(raw.SubscribeProperties{}), // properties
				gomock.Eq(uint64(1)),                                 // offset
			),
			fakeRawClient.EXPECT().
				NotifyChunk(gomock.AssignableToTypeOf(chunkChan)))
		opts := &ConsumerOptions{
			OffsetType:     uint16(1),
			SubscriptionId: uint8(1),
			Credit:         uint16(2),
			Offset:         uint64(1),
		}
		consumer, _ := NewConsumer("test-stream", fakeRawClient, handleMessages, opts)
		Expect(consumer).ToNot(BeNil())

		//test
		Expect(consumer.Subscribe(context.Background())).To(Succeed())
	})

	It("unsubscribes from the stream", func() {
		//setup
		var count int32
		handleMessages := func(ctxType context.Context, message *amqp.Message) {
			if atomic.AddInt32(&count, 1)%1000 == 0 {
				fmt.Printf("cousumed %d  messages \n", atomic.LoadInt32(&count))
				// AVOID to store for each single message, it will reduce the performances
				// The server keeps the consume tracking using the consumer name
			}
		}
		fakeRawClient.EXPECT().
			Unsubscribe(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf(uint8(1)))

		opts := &ConsumerOptions{
			OffsetType:     uint16(1),
			SubscriptionId: uint8(1),
			Credit:         uint16(2),
			Offset:         uint64(1),
		}
		//test
		consumer, _ := NewConsumer("test-stream", fakeRawClient, handleMessages, opts)
		Expect(consumer).ToNot(BeNil())
		Expect(consumer.Close(context.Background())).To(Succeed())

	})

	Describe("Single Active Consumer", func() {

	})

	Describe("SuperStream Consumer", func() {

	})

})
