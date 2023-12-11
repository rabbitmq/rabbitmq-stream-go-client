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

var _ = Describe("ConsumerManager", func() {
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
		})
		cm.client = fakeRawClient

		messagesHandler := func(consumerContext ConsumerContext, message *amqp.Message) {
			fmt.Printf("messages: %s\n", message.Data)
		}
		opts := &ConsumerOptions{}
		c, err := cm.createConsumer("consumer-manager-stream", messagesHandler, opts)
		Expect(err).NotTo(HaveOccurred())
		Expect(c).ToNot(BeNil())

		// the same raw client is shared across all consumers
		Expect(c.rawClient).To(Equal(cm.client))
		Expect(c.rawClientMu).To(Equal(cm.clientMu))
		Expect(cm.consumers).To(ContainElement(c))

		c2, err := cm.createConsumer("consumer-manager-stream", messagesHandler, opts)
		Expect(err).NotTo(HaveOccurred())
		Expect(c2.rawClient).To(Equal(cm.client))
		Expect(c2.rawClientMu).To(Equal(cm.clientMu))
		Expect(cm.consumers).To(ContainElement(c2))

		c3, err := cm.createConsumer("consumer-manager-stream", messagesHandler, opts)
		Expect(err).NotTo(HaveOccurred())
		Expect(c3.rawClient).To(Equal(cm.client))
		Expect(c3.rawClientMu).To(Equal(cm.clientMu))
		Expect(cm.consumers).To(ContainElement(c3))
	})

	When("consumer manager is full", func() {
		It("errors during consumer creation", func() {
			cm := newConsumerManager(EnvironmentConfiguration{
				MaxConsumersByConnection: 1,
			})
			cm.client = fakeRawClient

			messagesHandler := func(consumerContext ConsumerContext, message *amqp.Message) {
				fmt.Printf("messages: %s\n", message.Data)
			}
			opts := &ConsumerOptions{}
			_, err := cm.createConsumer("consumer-manager-stream", messagesHandler, opts)
			Expect(err).To(MatchError(ContainSubstring("consumer manager is full")))

		})
	})
})

func prepareMockNotifyPublish(c *MockRawClient) {
	chType := make(chan *raw.Chunk)
	chunkCh := reflect.TypeOf(chType)
	c.EXPECT().NotifyChunk(
		gomock.AssignableToTypeOf(chunkCh))
}
