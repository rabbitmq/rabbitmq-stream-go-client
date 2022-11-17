package raw_test

import (
	"context"
	"github.com/go-logr/logr"
	"github.com/golang/mock/gomock"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"net"
	"time"
)

var _ = Describe("Client", func() {
	var (
		fakeServerConn net.Conn
		fakeClientConn net.Conn
		mockCtrl       *gomock.Controller
		fakeConn       *MockConn
		fakeRabbitMQ   *fakeRabbitMQServer
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		fakeConn = NewMockConn(mockCtrl)
		fakeServerConn, fakeClientConn = net.Pipe()
		fakeRabbitMQ = &fakeRabbitMQServer{
			correlationIdSeq: autoIncrementingSequence{0},
			connection:       fakeServerConn,
			deadlineDelta:    time.Second,
		}
	})

	AfterEach(func() {
		Expect(fakeClientConn.Close()).To(Succeed())
		Expect(fakeServerConn.Close()).To(Succeed())
	})

	It("configures the client", func() {
		// the connection should not be used until Connect()
		fakeConn.
			EXPECT().
			Read(gomock.Any()).
			Times(0)

		By("creating a new raw client")
		conf, err := raw.NewClientConfiguration()
		Expect(err).ToNot(HaveOccurred())

		streamClient := raw.NewClient(fakeConn, conf)
		Expect(streamClient).NotTo(BeNil())

		By("not starting a connection yet")
		Consistently(streamClient.IsOpen).WithTimeout(time.Millisecond*200).Should(BeFalse(),
			"expected stream client to not be open")
	})

	It("establishes and closes a connection to RabbitMQ", func(ctx SpecContext) {
		// setup fake server responses
		go fakeRabbitMQ.fakeRabbitMQConnectionOpen(ctx)
		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())

		itCtx, cancel := context.WithTimeout(logr.NewContext(ctx, GinkgoLogr), time.Second*4)
		defer cancel()

		conf, err := raw.NewClientConfiguration()
		Expect(err).ToNot(HaveOccurred())

		streamClient := raw.NewClient(fakeClientConn, conf)
		Eventually(streamClient.Connect).
			WithContext(itCtx).
			WithTimeout(time.Second).
			Should(Succeed(), "expected connection to succeed")
		Consistently(streamClient.IsOpen).
			WithTimeout(time.Second).
			Should(BeTrue(), "expected connection to be open")

		go fakeRabbitMQ.fakeRabbitMQConnectionClose(itCtx)
		// We need to renew the deadline
		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())

		Eventually(streamClient.Close).
			WithContext(itCtx).
			WithTimeout(time.Second).
			Should(Succeed())
		Consistently(streamClient.IsOpen).
			WithTimeout(time.Second).
			Should(BeFalse(), "expected connection to be closed")
	})

	It("creates a new stream", func(ctx SpecContext) {
		itCtx, cancel := context.WithTimeout(logr.NewContext(ctx, GinkgoLogr), time.Second*3)
		defer cancel()

		conf, err := raw.NewClientConfiguration()
		Expect(err).ToNot(HaveOccurred())

		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
		streamClient := raw.NewClient(fakeClientConn, conf)
		go streamClient.(*raw.Client).StartFrameListener(itCtx)

		go fakeRabbitMQ.fakeRabbitMQDeclareStream(newContextWithResponseCode(itCtx, 0x0001))

		err = streamClient.DeclareStream(itCtx, "test-stream", map[string]string{"some-key": "some-value"})
		Expect(err).To(Succeed())
	})

	It("cancels requests after a timeout", func(ctx SpecContext) {
		conf, err := raw.NewClientConfiguration()
		Expect(err).ToNot(HaveOccurred())

		routineCtx, rCancel := context.WithDeadline(ctx, time.Now().Add(time.Second*2))
		defer rCancel()
		go bufferDrainer(routineCtx, fakeServerConn)

		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
		streamClient := raw.NewClient(fakeClientConn, conf)
		connectCtx, cancel := context.WithDeadline(logr.NewContext(ctx, GinkgoLogr), time.Now().Add(time.Millisecond*500))
		defer cancel()

		Expect(streamClient.Connect(connectCtx)).To(MatchError("timed out waiting for server response"))
	}, SpecTimeout(2*time.Second))
})
