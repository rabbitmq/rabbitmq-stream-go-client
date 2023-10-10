package stream

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"go.uber.org/mock/gomock"
	"reflect"
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

	It("consume messages from the first offset", func() {
		//setup
		gomock.InOrder(fakeRawClient.EXPECT().
			Subscribe(
				gomock.AssignableToTypeOf(ctxType),                   //context
				gomock.Eq("test-stream"),                             // stream
				gomock.Eq(uint16(1)),                                 // offsetType
				gomock.Eq(uint8(1)),                                  // subscriptionId
				gomock.Eq(uint16(2)),                                 // credit
				gomock.AssignableToTypeOf(raw.SubscribeProperties{}), // properties
				gomock.Eq(uint64(1)),                                 // offset
			))
		opts := &ConsumerOptions{
			OffsetType:     uint16(1),
			SubscriptionId: uint8(1),
			Credit:         uint16(2),
			Offset:         uint64(1),
		}
		consumer := NewConsumer("test-stream", fakeRawClient, opts)
		Expect(consumer).ToNot(BeNil())

		//test
		Expect(consumer.Subscribe(context.Background())).To(Succeed())

	})

	Describe("Single Active Consumer", func() {

	})

	Describe("SuperStream Consumer", func() {

	})

})
