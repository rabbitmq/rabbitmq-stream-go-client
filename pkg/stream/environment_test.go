package stream_test

import (
	"context"
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/stream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"golang.org/x/exp/slog"
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

	Context("delete stream", func() {
		It("deletes a stream", func() {
			// setup
			mockRawClient.EXPECT().
				DeleteStream(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("my-stream"))
			mockRawClient.EXPECT().
				IsOpen().
				Return(true) // from maybeInitializeLocator

			err := environment.DeleteStream(rootCtx, "my-stream")
			Expect(err).ToNot(HaveOccurred())
		})

		When("there's an error", func() {
			BeforeEach(func() {
				// setup
				mockRawClient.EXPECT().
					DeleteStream(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("my-stream")).
					Return(errors.New("something went wrong")).
					AnyTimes()
				mockRawClient.EXPECT().
					IsOpen().
					Return(true) // from maybeInitializeLocator
			})

			It("bubbles up the error", func() {
				err := environment.DeleteStream(rootCtx, "error-in-delete")
				Expect(err).To(HaveOccurred())
			})

			It("logs locator operation errors", func() {
				// setup
				logBuffer := gbytes.NewBuffer()
				logger := slog.New(slog.NewTextHandler(logBuffer))
				ctx := raw.NewContextWithLogger(context.Background(), *logger)

				// act
				err := environment.DeleteStream(ctx, "log-errors")
				Expect(err).To(HaveOccurred())

				Eventually(logBuffer).Within(time.Second).Should(gbytes.Say(`"locator operation failed" error="something went wrong"`))
			})
		})

		When("there are multiple locators", func() {
			// marked as flaky because the environment picks a locator randomly
			// the test flakes if locator2 is picked first
			It("uses different locators when one fails", FlakeAttempts(3), func() {
				// setup
				locator2rawClient := stream.NewMockRawClient(mockCtrl)
				environment.AppendLocatorRawClient(locator2rawClient)
				environment.SetBackoffPolicy(func(_ int) time.Duration {
					return time.Millisecond * 10
				})
				locator2rawClient.EXPECT().
					DeleteStream(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string"))

				mockRawClient.EXPECT().
					DeleteStream(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string")).
					Return(errors.New("something went wrong")).
					Times(3)

				mockRawClient.EXPECT().
					IsOpen().
					Return(true) // from maybeInitializeLocator
				locator2rawClient.EXPECT().
					IsOpen().
					Return(true)

				// act
				err := environment.DeleteStream(rootCtx, "retried-delete-stream")
				Expect(err).ToNot(HaveOccurred())
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
