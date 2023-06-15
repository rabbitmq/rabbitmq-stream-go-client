package stream_test

import (
	"context"
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/stream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"reflect"
	"sync"
	"time"
)

var _ = Describe("Environment", func() {

	var (
		mockCtrl      *gomock.Controller
		mockRawClient *stream.MockRawClient
		environment   *stream.Environment
		rootCtx       = context.Background()
		ctxType       = reflect.TypeOf((*context.Context)(nil)).Elem()
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockRawClient = stream.NewMockRawClient(mockCtrl)

		c := stream.NewEnvironmentConfiguration(
			stream.WithLazyInitialization(true),
			stream.WithUri("fakehost:1234"),
		)
		var err error
		environment, err = stream.NewEnvironment(rootCtx, c)
		Expect(err).ToNot(HaveOccurred())

		environment.AppendLocatorRawClient(mockRawClient)
		environment.SetBackoffPolicy(func(_ int) time.Duration {
			return time.Millisecond * 10
		})
	})

	Context("create stream", func() {
		It("creates a stream", func() {
			// setup
			mockRawClient.EXPECT().DeclareStream(
				gomock.AssignableToTypeOf(ctxType),
				gomock.Eq("my-stream"),
				gomock.AssignableToTypeOf(raw.StreamConfiguration{}),
			)
			mockRawClient.EXPECT().
				IsOpen().
				Return(true) // from maybeInitializeLocator

			// act
			err := environment.CreateStream(context.Background(), "my-stream", stream.StreamOptions{})

			// assert
			Expect(err).ToNot(HaveOccurred())
		})

		It("creates a stream with given parameters", func() {
			// setup
			mockRawClient.EXPECT().DeclareStream(
				gomock.AssignableToTypeOf(ctxType),
				gomock.Eq("my-stream-with-options"),
				gomock.Eq(raw.StreamConfiguration{
					"x-max-age":                       "120s",
					"x-max-length-bytes":              "200000000",
					"x-stream-max-segment-size-bytes": "100000",
				}),
			)
			mockRawClient.EXPECT().
				IsOpen().
				Return(true) // from maybeInitializeLocator

			// act
			err := environment.CreateStream(
				context.Background(),
				"my-stream-with-options",
				stream.CreateStreamOptions{
					MaxAge:         time.Minute * 2,
					MaxLength:      stream.Megabyte * 200,
					MaxSegmentSize: stream.Kilobyte * 100,
				},
			)

			// assert
			Expect(err).ToNot(HaveOccurred())
		})

		It("bubbles up any error", func() {
			// setup
			mockRawClient.EXPECT().
				DeclareStream(
					gomock.AssignableToTypeOf(ctxType),
					gomock.AssignableToTypeOf("string"),
					gomock.AssignableToTypeOf(raw.StreamConfiguration{}),
				).
				AnyTimes().
				Return(errors.New("something went wrong"))
			mockRawClient.EXPECT().
				IsOpen().
				Return(true) // from maybeInitializeLocator

			// act
			err := environment.CreateStream(rootCtx, "a-stream", stream.CreateStreamOptions{})

			// assert
			Expect(err).To(MatchError("locator operation failed: something went wrong"))
		})

		When("the create stream operation errors", func() {
			// TODO: retries are tested at locator level. Do we need this test?
			It("retries", func() {
				// setup
				mockRawClient.EXPECT().
					DeclareStream(
						gomock.AssignableToTypeOf(ctxType),
						gomock.AssignableToTypeOf("string"),
						gomock.AssignableToTypeOf(raw.StreamConfiguration{}),
					).
					Return(errors.New("something went wrong")).
					Times(2)
				// it returns nil on the 3rd attempt
				mockRawClient.EXPECT().
					DeclareStream(
						gomock.AssignableToTypeOf(ctxType),
						gomock.AssignableToTypeOf("string"),
						gomock.AssignableToTypeOf(raw.StreamConfiguration{}),
					)
				mockRawClient.EXPECT().
					IsOpen().
					Return(true) // from maybeInitializeLocator

				// act
				err := environment.CreateStream(rootCtx, "retries-test", stream.CreateStreamOptions{})

				// assert
				Expect(err).ToNot(HaveOccurred())
			})

			It("gives up on non-retryable errors", func() {
				// setup
				mockRawClient.EXPECT().
					DeclareStream(
						gomock.AssignableToTypeOf(ctxType),
						gomock.AssignableToTypeOf("string"),
						gomock.AssignableToTypeOf(raw.StreamConfiguration{}),
					).
					Return(raw.ErrStreamAlreadyExists)
				mockRawClient.EXPECT().
					IsOpen().
					Return(true) // from maybeInitializeLocator

				// act
				err := environment.CreateStream(rootCtx, "non-retryable-test", stream.CreateStreamOptions{})

				// assert
				Expect(err).To(MatchError("stream already exists"))
			})
		})
	})

	When("multiple routines use the environment", func() {
		// FIXME(Zerpet): I'm not sure how reliable is this test
		//   it does not trigger the race detector. It makes some
		//   sense that it doesn't, since all access is read-only
		It("access the locators safely", func(ctx SpecContext) {
			// setup
			mockRawClient.EXPECT().
				DeclareStream(
					gomock.AssignableToTypeOf(ctxType),
					gomock.AssignableToTypeOf("string"),
					gomock.AssignableToTypeOf(raw.StreamConfiguration{}),
				).
				Times(10)
			mockRawClient.EXPECT().
				IsOpen().
				Return(true).
				Times(10) // from maybeInitializeLocator

			wg := sync.WaitGroup{}
			for i := 0; i < 10; i++ {
				wg.Add(1)
				go func() {
					_ = environment.CreateStream(ctx, "go-routine-stream", stream.CreateStreamOptions{})
					wg.Done()
				}()
			}

			wg.Wait()
		}, SpecTimeout(time.Second*2))
	})
})
