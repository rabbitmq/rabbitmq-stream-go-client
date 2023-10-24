package stream

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/common"
	"go.uber.org/mock/gomock"
	"reflect"
	"sync"
	"time"
)

var _ = Describe("Smart Producer", func() {

	var (
		mockController *gomock.Controller
		fakeRawClient  *MockRawClient
		ctxType        = reflect.TypeOf((*context.Context)(nil)).Elem()
	)

	BeforeEach(func() {
		mockController = gomock.NewController(GinkgoT())
		fakeRawClient = NewMockRawClient(mockController)
	})

	Describe("send batch", func() {
		var (
			p *standardProducer
		)

		AfterEach(func() {
			p.close()
		})

		When("the batch list is empty", func() {
			It("returns an error", func() {
				p = newStandardProducer(0, fakeRawClient, &ProducerOptions{})
				Expect(p.SendBatch(context.Background(), []amqp.Message{})).To(MatchError("batch list is empty"))
			})
		})

		It("sends messages batched by the user", func() {
			// setup
			var capturedPublishingIds []uint64
			gomock.InOrder(fakeRawClient.EXPECT().
				Send(
					gomock.AssignableToTypeOf(ctxType),
					gomock.Eq(uint8(1)),
					gomock.All(
						gomock.Len(1),
						gomock.AssignableToTypeOf([]common.PublishingMessager{}),
					),
				).
				Do(func(_ context.Context, _ uint8, pMessages []common.PublishingMessager) error {
					capturedPublishingIds = []uint64{pMessages[0].PublishingId()}
					return nil
				}),
				fakeRawClient.EXPECT().
					Send(
						gomock.AssignableToTypeOf(ctxType),
						gomock.Eq(uint8(1)),
						gomock.All(
							gomock.Len(3),
							gomock.AssignableToTypeOf([]common.PublishingMessager{}),
						),
					).
					Do(func(_ context.Context, _ uint8, pMessages []common.PublishingMessager) error {
						capturedPublishingIds = make([]uint64, 0, 3)
						for i := 0; i < len(pMessages); i++ {
							capturedPublishingIds = append(capturedPublishingIds, pMessages[i].PublishingId())
						}
						return nil
					}),
			)

			p = newStandardProducer(1, fakeRawClient, &ProducerOptions{
				MaxInFlight:         100,
				MaxBufferedMessages: 100,
			})

			// test
			batch := []amqp.Message{{Data: []byte("message 1")}}
			Expect(p.SendBatch(context.Background(), batch)).To(Succeed())
			Expect(capturedPublishingIds).To(ConsistOf(uint64(0)))

			batch = append(batch, amqp.Message{Data: []byte("message 2")}, amqp.Message{Data: []byte("message 3")})
			Expect(p.SendBatch(context.Background(), batch)).To(Succeed())
			Expect(capturedPublishingIds).To(ConsistOf(uint64(1), uint64(2), uint64(3)))
		})

		When("the batch list is larger than max in flight", func() {
			It("returns an error", func() {
				p = newStandardProducer(0, fakeRawClient, &ProducerOptions{
					MaxInFlight:         1,
					MaxBufferedMessages: 1,
				})
				msgs := make([]amqp.Message, 10)
				Expect(p.SendBatch(context.Background(), msgs)).To(MatchError(ErrBatchTooLarge))
			})
		})

		When("the pending confirmations + the batch is larger than max in flight", func() {
			It("returns an error", func() {
				Skip("TODO")
			})
		})
	})

	Describe("send with ID", func() {
		It("always returns an error", func() {
			p := &standardProducer{}
			Expect(
				p.SendWithId(context.Background(), 123, amqp.Message{Data: []byte("this will return an error")}),
			).To(MatchError(ErrUnsupportedOperation))
		})
	})

	Describe("send", func() {
		var (
			p *standardProducer
		)

		AfterEach(func() {
			p.close()
		})

		It("accumulates and sends messages", func() {
			m := &sync.Mutex{}
			var capturedIds = make([]uint64, 0)
			fakeRawClient.EXPECT().
				Send(gomock.AssignableToTypeOf(ctxType), gomock.Eq(uint8(42)),
					gomock.All(
						gomock.Len(3),
						gomock.AssignableToTypeOf([]common.PublishingMessager{}),
					),
				).
				Do(func(_ context.Context, _ uint8, pMessages []common.PublishingMessager) error {
					m.Lock()
					for i := 0; i < len(pMessages); i++ {
						capturedIds = append(capturedIds, pMessages[i].PublishingId())
					}
					m.Unlock()
					return nil
				})

			p = newStandardProducer(42, fakeRawClient, &ProducerOptions{
				MaxInFlight:          5,
				MaxBufferedMessages:  5,
				BatchPublishingDelay: time.Millisecond * 200, // batching delay must be lower than Eventually's timeout
			})

			Expect(p.Send(context.Background(), amqp.Message{Data: []byte("message 1")})).To(Succeed())
			Expect(p.Send(context.Background(), amqp.Message{Data: []byte("message 2")})).To(Succeed())
			Expect(p.Send(context.Background(), amqp.Message{Data: []byte("message 3")})).To(Succeed())

			Eventually(func() []uint64 {
				m.Lock()
				defer m.Unlock()
				return capturedIds
			}).Within(time.Second * 1).WithPolling(time.Millisecond * 200).Should(ConsistOf(uint64(0), uint64(1), uint64(2)))
		})

		It("publishes messages when buffer is full", func() {
			// setup
			m := &sync.Mutex{}
			var capturedIds = make([]uint64, 0)
			fakeRawClient.EXPECT().
				Send(gomock.AssignableToTypeOf(ctxType), gomock.Eq(uint8(42)),
					gomock.All(
						gomock.Len(3),
						gomock.AssignableToTypeOf([]common.PublishingMessager{}),
					),
				).
				Do(func(_ context.Context, _ uint8, pMessages []common.PublishingMessager) error {
					m.Lock()
					for i := 0; i < len(pMessages); i++ {
						capturedIds = append(capturedIds, pMessages[i].PublishingId())
					}
					m.Unlock()
					return nil
				}).
				Times(2)

			p = newStandardProducer(42, fakeRawClient, &ProducerOptions{
				MaxInFlight:          10,
				MaxBufferedMessages:  3,
				BatchPublishingDelay: time.Minute, // long batch delay so that publishing happens because buffer is full
			})

			// act
			Expect(p.Send(context.Background(), amqp.Message{Data: []byte("message 1")})).To(Succeed())
			Expect(p.Send(context.Background(), amqp.Message{Data: []byte("message 2")})).To(Succeed())
			Expect(p.Send(context.Background(), amqp.Message{Data: []byte("message 3")})).To(Succeed())
			Expect(p.Send(context.Background(), amqp.Message{Data: []byte("message 4")})).To(Succeed())
			Expect(p.Send(context.Background(), amqp.Message{Data: []byte("message 5")})).To(Succeed())
			Expect(p.Send(context.Background(), amqp.Message{Data: []byte("message 6")})).To(Succeed())
			Eventually(func() []uint64 {
				m.Lock()
				defer m.Unlock()
				return capturedIds
			}).Within(time.Millisecond * 200).WithPolling(time.Millisecond * 20).Should(ConsistOf(
				uint64(0),
				uint64(1),
				uint64(2),
				uint64(3),
				uint64(4),
				uint64(5),
			))

		})

		Context("message confirmations", func() {
			It("calls the confirmation handler", func() {
				// setup
				pingBack := make(chan MessageConfirmation, 1)
				wait := make(chan struct{})
				fakeRawClient.EXPECT().
					Send(gomock.AssignableToTypeOf(ctxType),
						gomock.AssignableToTypeOf(uint8(42)),
						gomock.AssignableToTypeOf([]common.PublishingMessager{}),
					).DoAndReturn(func(_ context.Context, _ uint8, _ []common.PublishingMessager) error {
					close(wait)
					return nil
				})

				p = newStandardProducer(12, fakeRawClient, &ProducerOptions{
					MaxInFlight:          10,
					MaxBufferedMessages:  10,
					BatchPublishingDelay: time.Millisecond * 50, // we want fast batching
					EnqueueTimeout:       0,
					ConfirmationHandler: func(confirm *MessageConfirmation) {
						pingBack <- *confirm
					},
					stream: "test-stream",
				})

				// routines started, and should not be sending (there's nothing to send)
				Consistently(pingBack).ShouldNot(Receive())

				Expect(
					p.Send(context.Background(), amqp.Message{Data: []byte("rabbitmq is awesome")}),
				).To(Succeed())
				select {
				case <-wait:
				case <-time.After(time.Second):
					Fail("expected to be unblocked by the mock, but we are still waiting")
				}

				// faking the producer manager receiving and forwarding a 'publish confirm'
				p.confirmedPublish <- &publishConfirmOrError{
					publishingId: 0,
					statusCode:   1,
				}

				var mc MessageConfirmation
				Eventually(pingBack).Should(Receive(&mc))
				Expect(mc.stream).To(Equal("test-stream"))
				Expect(mc.publishingId).To(BeNumerically("==", 0))
				Expect(mc.status).To(Equal(Confirmed))
				Expect(mc.messages).To(HaveLen(1))
				Expect(mc.messages[0].Data).To(BeEquivalentTo("rabbitmq is awesome"))
			})

			When("the pending confirmations is greater or equal than max in flight", func() {
				It("does not send the message and keeps the message in the message buffer", func() {
					// setup
					wait := make(chan struct{})
					fakeRawClient.EXPECT().
						Send(gomock.AssignableToTypeOf(ctxType),
							gomock.AssignableToTypeOf(uint8(42)),
							gomock.AssignableToTypeOf([]common.PublishingMessager{}),
						).DoAndReturn(func(_ context.Context, _ uint8, _ []common.PublishingMessager) error {
						close(wait)
						return nil
					})

					p = newStandardProducer(12, fakeRawClient, &ProducerOptions{
						MaxInFlight:          3,
						MaxBufferedMessages:  3,
						BatchPublishingDelay: time.Millisecond * 1500, // we want to publish on max buffered
						EnqueueTimeout:       time.Millisecond * 10,   // we want to time out quickly
						ConfirmationHandler:  nil,
						stream:               "test-stream",
					})

					// act
					message := amqp.Message{Data: []byte("rabbitmq is the best messaging broker")}
					Expect(p.Send(context.Background(), message)).To(Succeed())
					Expect(p.Send(context.Background(), message)).To(Succeed())
					Expect(p.Send(context.Background(), message)).To(Succeed())

					select {
					case <-wait:
					case <-time.After(time.Second):
						Fail("time out waiting for the mock to unblock us")
					}

					Expect(p.Send(context.Background(), message)).To(Succeed())
					Consistently(p.accumulator.messages).
						WithPolling(time.Millisecond * 200).
						Within(time.Millisecond * 1600).
						Should(HaveLen(1)) // 3 messages were sent, 1 message is buffered
					Expect(p.unconfirmedMessage.messages).To(HaveLen(3)) // 3 messages were sent, 3 confirmations are pending
				})
			})
		})
	})
})
