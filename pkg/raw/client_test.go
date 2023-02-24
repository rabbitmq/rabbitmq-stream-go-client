package raw_test

import (
	"bufio"
	"context"
	"errors"
	"github.com/go-logr/logr"
	"github.com/golang/mock/gomock"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"net"
	"time"
)

const (
	streamResponseCodeOK uint16 = iota + 1
	streamResponseCodeStreamDoesNotExist
	streamResponseCodeSubscriptionIdAlreadyExists
	streamResponseCodeSubscriptionIdDoesNotExist
	streamResponseCodeStreamAlreadyExists
	streamResponseCodeStreamNotAvailable
	streamResponseCodeSASLMechanismNotSupported
	streamResponseCodeAuthFailure
	streamResponseCodeSASLError
	streamResponseCodeSASLChallenge
	streamResponseCodeSASLAuthFailureLoopback
	streamResponseCodeVirtualHostAccessFailure
	streamResponseCodeUnknownFrame
	streamResponseCodeFrameTooLarge
	streamResponseCodeInternalError
	streamResponseCodeAccessRefused
	streamResponseCodePreconditionFailed
	streamResponseCodePublisherDoesNotExist
	streamResponseCodeNoOffset
)

var _ = Describe("Client", func() {
	var (
		fakeServerConn net.Conn
		fakeClientConn net.Conn
		mockCtrl       *gomock.Controller
		fakeConn       *MockConn
		fakeRabbitMQ   *fakeRabbitMQServer
		conf           *raw.ClientConfiguration
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
		// conf can be "global" as long as tests do not modify it
		// if a test needs to modify the configuration, it shall
		// make a local copy and then modify the configuration
		conf, _ = raw.NewClientConfiguration()
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
		streamClient := raw.NewClient(fakeConn, conf)
		Expect(streamClient).NotTo(BeNil())

		By("not starting a connection yet")
		Consistently(streamClient.IsOpen).WithTimeout(time.Millisecond*200).Should(BeFalse(),
			"expected stream client to not be open")
	})

	Context("request", func() {
		When("the context is cancelled", func() {
			var ctx context.Context
			var fakeCommandWrite *MockCommandWrite

			BeforeEach(func() {
				fakeCommandWrite = NewMockCommandWrite(mockCtrl)
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(context.Background())
				cancel()
			})

			It("does not do any work", func() {
				fakeCommandWrite.
					EXPECT().
					Write(gomock.AssignableToTypeOf(&bufio.Writer{})).
					Return(0, errors.New("not expected call")).
					AnyTimes()

				client := raw.NewClient(fakeConn, conf)
				Expect(client.(*raw.Client).Request(ctx, fakeCommandWrite)).To(MatchError(context.Canceled))
			})
		})
	})

	It("establishes and closes a connection to RabbitMQ", func(ctx SpecContext) {
		// setup fake server responses
		go fakeRabbitMQ.fakeRabbitMQConnectionOpen(ctx)
		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())

		itCtx, cancel := context.WithTimeout(logr.NewContext(ctx, GinkgoLogr), time.Second*4)
		defer cancel()

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

		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
		streamClient := raw.NewClient(fakeClientConn, conf)
		go streamClient.(*raw.Client).StartFrameListener(itCtx)

		go fakeRabbitMQ.fakeRabbitMQDeclareStream(newContextWithResponseCode(itCtx, 0x0001), "test-stream", constants.StreamConfiguration{"some-key": "some-value"})

		Expect(streamClient.DeclareStream(itCtx, "test-stream", constants.StreamConfiguration{"some-key": "some-value"})).To(Succeed())
	})

	Context("credits", func() {
		It("sends credits to the server", func(ctx SpecContext) {
			Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
			streamClient := raw.NewClient(fakeClientConn, conf)

			go fakeRabbitMQ.fakeRabbitMQCredit(2, 100)

			Expect(streamClient.Credit(ctx, 2, 100)).To(Succeed())
		})

		When("sending credits for non-existing subscription", func() {
			It("returns an error", func(ctx SpecContext) {
				Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
				streamClient := raw.NewClient(fakeClientConn, conf)
				go streamClient.(*raw.Client).StartFrameListener(ctx)

				go fakeRabbitMQ.fakeRabbitMQCreditResponse(
					newContextWithResponseCode(ctx, streamResponseCodeSubscriptionIdDoesNotExist, "credit"),
					123,
				)
				var notification *raw.CreditError
				notificationCh := streamClient.NotifyCreditError(make(chan *raw.CreditError))
				Eventually(notificationCh).Should(Receive(&notification))
				Expect(notification).To(BeAssignableToTypeOf(&raw.CreditError{}))
				Expect(notification.ResponseCode()).To(BeNumerically("==", streamResponseCodeSubscriptionIdDoesNotExist))
				Expect(notification.SubscriptionId()).To(BeNumerically("==", 123))
			})
		})
	})

	Context("metadata", func() {
		It("sends a metadata query to the server", func(ctx SpecContext) {
			Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
			streamClient := raw.NewClient(fakeClientConn, conf)
			go streamClient.(*raw.Client).StartFrameListener(ctx)

			go fakeRabbitMQ.fakeRabbitMQMetadataQuery("stream-exists")

			metadataResponse, err := streamClient.MetadataQuery(ctx, "stream-exists")
			Expect(err).NotTo(HaveOccurred())
			Expect(metadataResponse.ResponseCode()).To(BeNumerically("==", 0))
			Expect(metadataResponse.CorrelationId()).To(BeNumerically("==", 1))
		})

		Context("when a metadata query is sent for a non existent stream name", func() {
			It("returns an error", func(ctx SpecContext) {
				Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
				streamClient := raw.NewClient(fakeClientConn, conf)
				go streamClient.(*raw.Client).StartFrameListener(ctx)

				go fakeRabbitMQ.fakeRabbitMQMetadataResponse(
					newContextWithResponseCode(ctx, streamResponseCodeStreamDoesNotExist, "metadata"),
					"stream-does-not-exist",
				)

				_, err := streamClient.MetadataQuery(ctx, "stream-does-not-exist")
				Expect(err).To(MatchError("stream does not exist"))
			})
		})
	})

	It("Delete a stream", func(ctx SpecContext) {
		itCtx, cancel := context.WithTimeout(logr.NewContext(ctx, GinkgoLogr), time.Second*3)
		defer cancel()

		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
		streamClient := raw.NewClient(fakeClientConn, conf)
		go streamClient.(*raw.Client).StartFrameListener(itCtx)

		go fakeRabbitMQ.fakeRabbitMQDeleteStream(newContextWithResponseCode(itCtx, 0x0001), "test-stream")
		Expect(streamClient.DeleteStream(itCtx, "test-stream")).To(Succeed())
	})

	It("Declare new Publisher", func(ctx SpecContext) {
		itCtx, cancel := context.WithTimeout(logr.NewContext(ctx, GinkgoLogr), time.Second*3)
		defer cancel()

		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
		streamClient := raw.NewClient(fakeClientConn, conf)
		go streamClient.(*raw.Client).StartFrameListener(itCtx)

		go fakeRabbitMQ.fakeRabbitMQNewPublisher(newContextWithResponseCode(itCtx, 0x0001), 12, "myPublisherRef", "test-stream")
		Expect(streamClient.DeclarePublisher(itCtx, 12, "myPublisherRef", "test-stream")).To(Succeed())
	})

	It("Delete Publisher", func(ctx SpecContext) {
		itCtx, cancel := context.WithTimeout(logr.NewContext(ctx, GinkgoLogr), time.Second*3)
		defer cancel()

		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
		streamClient := raw.NewClient(fakeClientConn, conf)
		go streamClient.(*raw.Client).StartFrameListener(itCtx)

		go fakeRabbitMQ.fakeRabbitMQDeletePublisher(newContextWithResponseCode(itCtx, 0x0001), 12)
		Expect(streamClient.DeletePublisher(itCtx, 12)).To(Succeed())
	})

	It("receives messages", func(ctx SpecContext) {
		itCtx, cancel := context.WithTimeout(logr.NewContext(ctx, GinkgoLogr), time.Second*3)
		defer cancel()

		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
		streamClient := raw.NewClient(fakeClientConn, conf)
		go streamClient.(*raw.Client).StartFrameListener(itCtx)

		go fakeRabbitMQ.fakeRabbitMQNewConsumer(newContextWithResponseCode(itCtx, 0x0001), 12, "mystream",
			constants.OffsetTypeOffset, 60_001, 5,
			constants.SubscribeProperties{"some-config": "it-works"})

		By("subscribing to a stream")
		// must register channel before subscribing
		delivery := streamClient.NotifyChunk(make(chan *raw.Chunk, 1))
		Expect(streamClient.Subscribe(
			itCtx,
			"mystream",
			constants.OffsetTypeOffset,
			12,
			5,
			constants.SubscribeProperties{"some-config": "it-works"},
			60_001,
		)).To(Succeed())

		By("registering a channel")
		var someChunk *raw.Chunk
		Eventually(delivery).Should(Receive(&someChunk))
		Expect(someChunk.Messages).To(BeEquivalentTo("hello"))
		Expect(someChunk.SubscriptionId).To(BeNumerically("==", 12))
	})

	It("exchanges commands information", func(ctx SpecContext) {
		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
		streamClient := raw.NewClient(fakeClientConn, conf)
		go streamClient.(*raw.Client).StartFrameListener(ctx)

		go fakeRabbitMQ.fakeRabbitMQExchangeCommandVersions(ctx)

		Expect(streamClient.ExchangeCommandVersions(ctx)).To(Succeed())
	}, SpecTimeout(time.Second*3))

	It("cancels requests after a timeout", func(ctx SpecContext) {
		// This test does not start a fake to mimic rabbitmq responses. By not starting a
		// fake rabbitmq, we simulate "rabbit not responding". The expectation is to
		// receive a timeout error
		routineCtx, rCancel := context.WithDeadline(ctx, time.Now().Add(time.Second*2))
		defer rCancel()
		go bufferDrainer(routineCtx, fakeServerConn)

		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
		streamClient := raw.NewClient(fakeClientConn, conf)
		connectCtx, cancel := context.WithDeadline(logr.NewContext(ctx, GinkgoLogr), time.Now().Add(time.Millisecond*500))
		defer cancel()

		Expect(streamClient.Connect(connectCtx)).To(MatchError("timed out waiting for server response"))
	}, SpecTimeout(2*time.Second))

	Context("server returns an error", func() {
		BeforeEach(func() {
			Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
		})

		When("stream already exists", func() {
			It("returns a 'stream already exists' error", func(ctx SpecContext) {
				ctx2 := newContextWithResponseCode(logr.NewContext(ctx, GinkgoLogr), streamResponseCodeStreamAlreadyExists)

				streamClient := raw.NewClient(fakeClientConn, conf)
				go streamClient.(*raw.Client).StartFrameListener(ctx2)

				go fakeRabbitMQ.fakeRabbitMQDeclareStream(ctx2, "already-exists", constants.StreamConfiguration{})
				Expect(streamClient.DeclareStream(ctx, "already-exists", constants.StreamConfiguration{})).To(MatchError("stream already exists"))
			}, SpecTimeout(1500*time.Millisecond))
		})

		When("stream does not exist", func() {
			It("returns 'stream does not exist' error", func(ctx SpecContext) {
				ctx2 := newContextWithResponseCode(logr.NewContext(ctx, GinkgoLogr), streamResponseCodeStreamDoesNotExist)

				streamClient := raw.NewClient(fakeClientConn, conf)
				go streamClient.(*raw.Client).StartFrameListener(ctx2)

				By("deleting a non-existent stream")
				go fakeRabbitMQ.fakeRabbitMQDeleteStream(ctx2, "does-not-exist")
				Expect(streamClient.DeleteStream(ctx2, "does-not-exist")).To(MatchError("stream does not exist"))

				By("declaring a publisher to a non-existent stream")
				go fakeRabbitMQ.fakeRabbitMQNewPublisher(ctx2, 123, "a-publisher", "not-here")
				Expect(streamClient.DeclarePublisher(ctx2, 123, "a-publisher", "not-here")).
					To(MatchError("stream does not exist"))
			}, SpecTimeout(1500*time.Millisecond))
		})

		When("authentication fails", func() {
			It("returns 'authentication failed' error", func(ctx SpecContext) {
				itCtx := logr.NewContext(newContextWithResponseCode(ctx, streamResponseCodeAuthFailure, "sasl-auth"), GinkgoLogr)
				go fakeRabbitMQ.fakeRabbitMQConnectionOpen(itCtx)

				streamClient := raw.NewClient(fakeClientConn, conf)
				Expect(streamClient.Connect(itCtx)).
					To(MatchError("authentication failure"))
				Expect(streamClient.IsOpen()).
					Should(BeFalse(), "expected connection to be closed")
			}, SpecTimeout(1500*time.Millisecond))
		})
	})

	When("the context is cancelled", func() {
		var client raw.Clienter

		BeforeEach(func() {
			client = raw.NewClient(fakeClientConn, conf)
		})

		It("does not start any work", func(ctx SpecContext) {
			ctx2, cancel := context.WithCancel(ctx)
			cancel()

			By("not blocking in connect")
			Expect(client.Connect(ctx2)).To(MatchError("context canceled"))

			By("not blocking in stream declaration")
			Expect(client.DeclareStream(ctx2, "not-created", constants.StreamConfiguration{})).
				To(MatchError("context canceled"))

			By("not blocking on stream deletion")
			Expect(client.DeleteStream(ctx2, "who-dat")).To(MatchError("context canceled"))

			By("not blocking on declare publisher")
			Expect(client.DeclarePublisher(ctx2, 123, "a-publisher", "some-stream")).
				To(MatchError("context canceled"))
		}, SpecTimeout(500*time.Millisecond))
	})

	When("the server closes the connection", func() {
		It("responds back and shutdowns", func(ctx SpecContext) {
			Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())
			streamClient := raw.NewClient(fakeClientConn, conf)
			streamClient.(*raw.Client).SetIsOpen(true)

			routineCtx := logr.NewContext(ctx, GinkgoLogr)
			go streamClient.(*raw.Client).StartFrameListener(routineCtx)

			go fakeRabbitMQ.fakeRabbitMQServerClosesConnection()
			Eventually(streamClient.IsOpen).
				WithTimeout(500*time.Millisecond).
				WithPolling(100*time.Millisecond).
				Should(BeFalse(), "expected connection to be closed")
		})
	})

	It("receives publish confirmations", func(ctx SpecContext) {
		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second * 2))).To(Succeed())
		streamClient := raw.NewClient(fakeClientConn, conf)

		confirmsCh := make(chan *raw.PublishConfirm, 1)
		streamClient.NotifyPublish(confirmsCh)

		go streamClient.(*raw.Client).StartFrameListener(ctx)
		fakeRabbitMQ.fakeRabbitMQPublisherConfirms(1, 5)

		eventuallyCtx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		// Had to mimic the 'eventually' behaviour because Eventually().Should(Receive()) is not passing,
		// even though the channel is receiving
		//Eventually(confirmsCh).WithTimeout(time.Second * 3).Should(Receive())
		select {
		case <-eventuallyCtx.Done():
			Fail("did not receive from confirmation channel")
		case c := <-confirmsCh:
			Expect(c.PublisherID()).To(BeNumerically("==", 1))
			Expect(c.PublishingIds()).To(HaveLen(5))
			Expect(c.PublishingIds()).To(ConsistOf([]uint64{0, 1, 2, 3, 4}))
		}
	}, SpecTimeout(3*time.Second))

	When("the connection closes", func() {
		It("closes the confirmation channel", func(ctx SpecContext) {
			streamClient := raw.NewClient(fakeClientConn, conf)
			streamClient.(*raw.Client).SetIsOpen(true)

			go streamClient.(*raw.Client).StartFrameListener(ctx)

			c := make(chan *raw.PublishConfirm)
			streamClient.NotifyPublish(c)
			Expect(c).ToNot(BeClosed())

			go fakeRabbitMQ.fakeRabbitMQConnectionClose(ctx)
			Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())

			Expect(streamClient.Close(ctx)).To(Succeed())
			Expect(c).To(BeClosed())
		}, SpecTimeout(time.Second*2))
	})
})
