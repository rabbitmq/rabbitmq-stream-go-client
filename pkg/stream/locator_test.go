package stream

import (
	"context"
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"golang.org/x/exp/slog"
	"reflect"
	"time"
)

var _ = Describe("Locator", func() {
	var (
		logger     *slog.Logger
		mockCtrl   *gomock.Controller
		mockClient *MockRawClient
		ctxType    = reflect.TypeOf((*context.Context)(nil)).Elem()
		rootCtx    = context.Background()
	)

	BeforeEach(func() {
		logger = slog.New(slog.NewTextHandler(GinkgoWriter))
		mockCtrl = gomock.NewController(GinkgoT())
		mockClient = NewMockRawClient(mockCtrl)
	})

	It("reconnects", func() {
		Skip("this needs a real rabbit to test, or close enough to real rabbit")
	})

	Context("stream declaration", func() {
		var (
			l *locator
		)

		BeforeEach(func() {
			l = &locator{
				log:                  logger,
				shutdownNotification: make(chan struct{}),
				Client:               mockClient,
				isSet:                true,
				backOffPolicy: func(_ int) time.Duration {
					return time.Millisecond * 10
				},
			}
		})

		It("creates a stream", func() {
			// setup
			mockClient.EXPECT().
				DeclareStream(
					gomock.AssignableToTypeOf(ctxType),
					gomock.AssignableToTypeOf("string"),
					gomock.AssignableToTypeOf(raw.StreamConfiguration{}),
				)

			// act
			err := l.createStream(rootCtx, "my-stream", raw.StreamConfiguration{})

			// assert
			Expect(err).ToNot(HaveOccurred())
		})

		It("bubbles up create errors", func() {
			// setup
			mockClient.EXPECT().
				DeclareStream(
					gomock.AssignableToTypeOf(ctxType),
					gomock.AssignableToTypeOf("string"),
					gomock.AssignableToTypeOf(raw.StreamConfiguration{}),
				).
				Return(errors.New("something went wrong")).
				AnyTimes()

			// act
			err := l.createStream(rootCtx, "oopsie", raw.StreamConfiguration{})

			// assert
			Expect(err).To(MatchError("something went wrong"))
		})

		When("the create stream operation errors", func() {
			It("retries", func() {
				// setup
				mockClient.EXPECT().
					DeclareStream(
						gomock.AssignableToTypeOf(ctxType),
						gomock.AssignableToTypeOf("string"),
						gomock.AssignableToTypeOf(raw.StreamConfiguration{}),
					).
					Return(errors.New("something went wrong")).
					Times(2)
				mockClient.EXPECT().
					DeclareStream(
						gomock.AssignableToTypeOf(ctxType),
						gomock.AssignableToTypeOf("string"),
						gomock.AssignableToTypeOf(raw.StreamConfiguration{}),
					)

				// act
				err := l.createStream(rootCtx, "retryable-create", raw.StreamConfiguration{})

				// assert
				Expect(err).ToNot(HaveOccurred())
				// TODO: assert that it logs errors
			})
		})

		It("sets the locator", func() {
			Skip("this test needs a real rabbit, or close enough")
		})
	})
})
