package stream

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	"reflect"
)

var _ = Describe("ProducerManager", func() {

	var (
		mockCtrl      *gomock.Controller
		fakeRawClient *MockRawClient
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		fakeRawClient = NewMockRawClient(mockCtrl)
	})

	It("creates new producers", func() {
		prepareMockDeclarePublisher(fakeRawClient, 0)
		prepareMockDeclarePublisher(fakeRawClient, 1)
		prepareMockDeclarePublisher(fakeRawClient, 2)

		pm := newProducerManager(0, EnvironmentConfiguration{
			MaxProducersByConnection: 10,
		})
		pm.client = fakeRawClient
		pm.open = true

		p, err := pm.createProducer(context.Background(), &ProducerOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(p).ToNot(BeNil())

		// the same raw client must be shared among all producers in this manager
		Expect(p.(*standardProducer).rawClient).To(Equal(pm.client))
		Expect(p.(*standardProducer).rawClientMu).To(Equal(pm.clientM))
		Expect(p.(*standardProducer).publisherId).To(BeEquivalentTo(0))
		Expect(pm.producers).To(ContainElement(p))

		p2, err := pm.createProducer(context.Background(), &ProducerOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(p2.(*standardProducer).rawClient).To(Equal(pm.client))
		Expect(p2.(*standardProducer).rawClientMu).To(Equal(pm.clientM))
		Expect(p2.(*standardProducer).publisherId).To(BeEquivalentTo(1))
		Expect(pm.producers).To(ContainElement(p2))

		p3, err := pm.createProducer(context.Background(), &ProducerOptions{})
		Expect(err).ToNot(HaveOccurred())
		Expect(p3.(*standardProducer).rawClient).To(Equal(pm.client))
		Expect(p3.(*standardProducer).rawClientMu).To(Equal(pm.clientM))
		Expect(p3.(*standardProducer).publisherId).To(BeEquivalentTo(2))
		Expect(pm.producers).To(ContainElement(p3))
	})

	When("manager is full", func() {
		It("errors in producer creation", func() {
			prepareMockDeclarePublisher(fakeRawClient, 0)
			pm := newProducerManager(0, EnvironmentConfiguration{
				MaxProducersByConnection: 1,
			})
			pm.client = fakeRawClient
			pm.open = true

			_, err := pm.createProducer(context.Background(), &ProducerOptions{})
			Expect(err).ToNot(HaveOccurred())

			_, err = pm.createProducer(context.Background(), &ProducerOptions{})
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(ContainSubstring("producer manager is full")))

			Expect(pm.producers).To(HaveLen(1))
		})
	})

	When("manager is closed", func() {
		It("does not create new producers", func() {
			pm := newProducerManager(0, EnvironmentConfiguration{
				MaxProducersByConnection: 1,
			})
			pm.client = fakeRawClient
			pm.open = false

			_, err := pm.createProducer(context.Background(), &ProducerOptions{})
			Expect(err).To(MatchError(ContainSubstring("producer manager is closed")))
		})
	})

	When("producers are added and removed", func() {
		It("assigns the correct ID", func() {
			prepareMockDeclarePublisher(fakeRawClient, 0)
			prepareMockDeclarePublisher(fakeRawClient, 1)
			prepareMockDeclarePublisher(fakeRawClient, 1)
			prepareMockDeclarePublisher(fakeRawClient, 2)
			prepareMockDeletePublisher(fakeRawClient, 1)
			prepareMockDeletePublisher(fakeRawClient, 2)
			pm := newProducerManager(0, EnvironmentConfiguration{
				MaxProducersByConnection: 5,
			})
			pm.client = fakeRawClient
			pm.open = true

			By("reusing publisher ID 1")
			opts := &ProducerOptions{
				stream: "test-stream",
			}
			p, err := pm.createProducer(context.Background(), opts)
			Expect(err).ToNot(HaveOccurred())
			Expect(p.(*standardProducer).publisherId).To(BeEquivalentTo(0))

			p2, err := pm.createProducer(context.Background(), opts)
			Expect(err).ToNot(HaveOccurred())
			Expect(p2.(*standardProducer).publisherId).To(BeEquivalentTo(1))

			Expect(pm.removeProducer(1)).To(Succeed())

			p3, err := pm.createProducer(context.Background(), opts)
			Expect(err).ToNot(HaveOccurred())
			Expect(p3.(*standardProducer).publisherId).To(BeEquivalentTo(1))

			By("reusing the producer slot #1")
			Expect(pm.producers[1]).ToNot(BeNil())

			By("reusing publisher ID 2", func() {
				pm.producers = append(
					pm.producers,
					&standardProducer{publisherId: 2},
					&standardProducer{publisherId: 3},
					&standardProducer{publisherId: 4},
				)
			})
			Expect(pm.removeProducer(2)).To(Succeed())
			p4, err := pm.createProducer(context.Background(), opts)
			Expect(err).ToNot(HaveOccurred())
			Expect(p4.(*standardProducer).publisherId).To(BeEquivalentTo(2))

			By("reusing the producer slot #2")
			Expect(pm.producers[2]).ToNot(BeNil())
		})
	})

	When("last producer is closed", func() {
		It("closes the connection", func() {
			fakeRawClient.EXPECT().Close(gomock.Any())
			prepareMockDeclarePublisher(fakeRawClient, 0)
			prepareMockDeletePublisher(fakeRawClient, 0) // called during producer.Close() callback
			pm := newProducerManager(0, EnvironmentConfiguration{MaxProducersByConnection: 5})
			pm.client = fakeRawClient
			pm.open = true

			producer, err := pm.createProducer(context.Background(), &ProducerOptions{})
			Expect(err).ToNot(HaveOccurred())
			producer.Close()

			Eventually(func() Producer {
				pm.m.Lock()
				defer pm.m.Unlock()
				return pm.producers[0]
			}, "1s", "200ms").Should(BeNil())
		})
	})
})

func prepareMockDeclarePublisher(m *MockRawClient, publisherId uint8) {
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	m.EXPECT().DeclarePublisher(
		gomock.AssignableToTypeOf(ctxType),
		gomock.Eq(publisherId),
		gomock.AssignableToTypeOf(""),
		gomock.AssignableToTypeOf(""),
	)
}

func prepareMockDeletePublisher(m *MockRawClient, publisherId uint8) {
	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	m.EXPECT().DeletePublisher(
		gomock.AssignableToTypeOf(ctxType),
		gomock.Eq(publisherId),
	)
}
