package stream

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"go.uber.org/mock/gomock"
	"reflect"
)

// Responsibilies of Consumer Manager
// - Creates consumers up to maximum consumers
// - Creates notifychunk channel for receiveing messages
// - dispatches to messages to consumers in round robin fashion
// - handles state of consumers in SAC scenarios

var _ = FDescribe("ConsumerManager", func() {
	var (
		mockCtrl      *gomock.Controller
		fakeRawClient *MockRawClient
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		fakeRawClient = NewMockRawClient(mockCtrl)
	})

	It("creates new consumers", func() {
		prepareMockNotifyPublish(fakeRawClient)
		cm := newConsumerManager(EnvironmentConfiguration{
			MaxConsumersByConnection: 10,
		}, fakeRawClient)

		messagesHandler := func(consumerContext ConsumerContext, message *amqp.Message) {
			fmt.Printf("messages: %s\n", message.Data)
		}
		opts := &ConsumerOptions{}
		err := cm.createConsumer("consumer-manager-stream", messagesHandler, opts)
		Expect(err).NotTo(HaveOccurred())

		// the same raw client is shared across all consumers
		Expect(cm.consumers[0].rawClient).To(Equal(cm.client))
		Expect(cm.consumers[0].rawClientMu).To(Equal(cm.clientMu))

		err = cm.createConsumer("consumer-manager-stream", messagesHandler, opts)
		Expect(err).NotTo(HaveOccurred())
		Expect(cm.consumers).To(HaveLen(2))
		Expect(cm.consumers[1].rawClient).To(Equal(cm.client))
		Expect(cm.consumers[1].rawClientMu).To(Equal(cm.clientMu))

		err = cm.createConsumer("consumer-manager-stream", messagesHandler, opts)
		Expect(err).NotTo(HaveOccurred())
		Expect(cm.consumers).To(HaveLen(3))
		Expect(cm.consumers[2].rawClient).To(Equal(cm.client))
		Expect(cm.consumers[2].rawClientMu).To(Equal(cm.clientMu))
	})

	When("consumer manager is full", func() {
		It("errors during consumer creation", func() {
			prepareMockNotifyPublish(fakeRawClient)
			cm := newConsumerManager(EnvironmentConfiguration{
				MaxConsumersByConnection: 1,
			}, fakeRawClient)

			messagesHandler := func(consumerContext ConsumerContext, message *amqp.Message) {
				fmt.Printf("messages: %s\n", message.Data)
			}
			opts := &ConsumerOptions{}
			err := cm.createConsumer("consumer-manager-stream", messagesHandler, opts)
			Expect(err).NotTo(HaveOccurred())
			err = cm.createConsumer("consumer-manager-stream", messagesHandler, opts)
			Expect(err).To(MatchError(ContainSubstring("consumer manager is full")))

		})
	})

	FWhen("a message is received", func() {
		It("dispatches the message to a consumer", func(ctx SpecContext) {
			//Create a manager with 2 consumers
			// mock receiveing some chunks
			// see that chunks a received by consumers but not duplicated

			prepareMockNotifyPublish(fakeRawClient)
			cm := newConsumerManager(EnvironmentConfiguration{
				MaxConsumersByConnection: 2,
			}, fakeRawClient)

			messagesHandler := func(consumerContext ConsumerContext, message *amqp.Message) {
				fmt.Printf("messages: %s\n", message.Data)
			}
			opts := &ConsumerOptions{}
			err := cm.createMaxConsumers("consumer-manager-stream", messagesHandler, opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(cm.consumers).To(HaveLen(2))

			// Subscribe to start receiveing messages
			err = cm.Subscribe(ctx)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

func prepareMockNotifyPublish(c *MockRawClient) {
	chType := make(chan *raw.Chunk)
	chunkCh := reflect.TypeOf(chType)
	c.EXPECT().NotifyChunk(
		gomock.AssignableToTypeOf(chunkCh))
}
