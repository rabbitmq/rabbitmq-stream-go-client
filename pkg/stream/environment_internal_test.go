//go:build rabbitmq.stream.test

package stream

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"go.uber.org/mock/gomock"
	"time"
)

var _ = Describe("Environment Internal Unit Test", Pending, func() {
	var (
		env             *Environment
		fakeRawClient   *MockRawClient
		mockCtrl        *gomock.Controller
		backOffPolicyFn = func(_ int) time.Duration {
			return time.Millisecond * 10
		}
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		fakeRawClient = NewMockRawClient(mockCtrl)

		c := NewEnvironmentConfiguration(
			WithLazyInitialization(true),
			WithUri("rabbitmq-stream://foo:bar@fakehost:1234/my-vhost"),
		)
		var err error
		env, err = NewEnvironment(context.Background(), c)
		Expect(err).ToNot(HaveOccurred())

		env.AppendLocatorRawClient(fakeRawClient)
		env.SetBackoffPolicy(backOffPolicyFn)
	})

	It("creates a new producer", func() {
		By("locating the stream leader", func() {
			fakeRawClient.EXPECT().
				IsOpen().Return(true)
			fakeRawClient.EXPECT().
				MetadataQuery(gomock.Any(), gomock.AssignableToTypeOf([]string{"string-slice"})).
				Return(raw.MetadataResponseForStream("some-stream"), nil)
			fakeRawClient.EXPECT().
				DeclarePublisher(
					gomock.Any(),
					gomock.AssignableToTypeOf(uint8(1)),
					gomock.AssignableToTypeOf("string"),
					gomock.AssignableToTypeOf("string"),
				)
		})
		p, err := env.CreateProducer(context.Background(), "some-stream", &ProducerOptions{})
		Expect(err).ToNot(HaveOccurred())

		By("creating a producer manager connected to the leader")
		Expect(env.producerManagers).To(HaveLen(1))
		Expect(env.producerManagers[0].config.Uris).To(ContainElement("rabbitmq-stream://foo:bar@fakehost:1234/my-vhost"))

		By("assigning a producer manager for the producer")
		Expect(env.producerManagers[0].producers).To(ContainElement(p))
	})

	It("returns an error for empty stream name", func() {
		Skip("TODO")
	})
})

func prepareMockForDeclarePublish(mock *MockRawClient, pubId uint8) {
	mock.EXPECT().
		IsOpen().
		Return(true) // from maybeInitializeLocator

	mock.EXPECT().
		DeclarePublisher(
			gomock.Any(),
			gomock.Eq(pubId),
			gomock.AssignableToTypeOf("string"),
			gomock.AssignableToTypeOf("string"),
		)
}
