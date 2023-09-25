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
		When("the batch list is empty", func() {
			It("returns an error", func() {
				p := standardProducer{
					publisherId:     0,
					rawClient:       fakeRawClient,
					rawClientMu:     &sync.Mutex{},
					publishingIdSeq: autoIncrementingSequence[uint64]{},
				}

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

			p := standardProducer{
				publisherId: 1,
				rawClient:   fakeRawClient,
				rawClientMu: &sync.Mutex{},
				opts:        ProducerOptions{100, 100},
			}

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
				p := standardProducer{
					publisherId:     0,
					rawClient:       fakeRawClient,
					rawClientMu:     &sync.Mutex{},
					publishingIdSeq: autoIncrementingSequence[uint64]{},
					opts:            ProducerOptions{1, 1},
				}
				msgs := make([]amqp.Message, 10)
				Expect(p.SendBatch(context.Background(), msgs)).To(MatchError(ErrBatchTooLarge))
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

			p := newStandardProducer(42, fakeRawClient, ProducerOptions{5, 5})

			Expect(p.Send(context.Background(), amqp.Message{Data: []byte("message 1")})).To(Succeed())
			Expect(p.Send(context.Background(), amqp.Message{Data: []byte("message 2")})).To(Succeed())
			Expect(p.Send(context.Background(), amqp.Message{Data: []byte("message 3")})).To(Succeed())

			Eventually(func() []uint64 {
				m.Lock()
				defer m.Unlock()
				return capturedIds
			}).Within(time.Second * 1).WithPolling(time.Millisecond * 200).Should(ConsistOf(uint64(0), uint64(1), uint64(2)))
		})

	})
})
