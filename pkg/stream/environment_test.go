package stream_test

import (
	"context"
	"errors"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/stream"
	"go.uber.org/mock/gomock"
	"golang.org/x/exp/slog"
	"reflect"
	"sync"
	"time"
)

var _ = Describe("Environment", func() {

	var (
		mockCtrl        *gomock.Controller
		mockRawClient   *stream.MockRawClient
		environment     *stream.Environment
		rootCtx         = context.Background()
		ctxType         = reflect.TypeOf((*context.Context)(nil)).Elem()
		backOffPolicyFn = func(_ int) time.Duration {
			return time.Millisecond * 10
		}
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
		environment.SetBackoffPolicy(backOffPolicyFn)
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
			err := environment.CreateStream(context.Background(), "my-stream", stream.CreateStreamOptions{})

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

		// marked as flaky because the environment picks a locator randomly
		// the test flakes if locator2 is picked first
		When("there are multiple locators", func() {
			var (
				locator2rawClient *stream.MockRawClient
			)

			BeforeEach(func() {
				locator2rawClient = stream.NewMockRawClient(mockCtrl)
				environment.AppendLocatorRawClient(locator2rawClient)
				environment.SetBackoffPolicy(backOffPolicyFn)
				environment.SetLocatorSelectSequential(true)

				mockRawClient.EXPECT().
					IsOpen().
					Return(true) // from maybeInitializeLocator
			})

			It("uses different locators when one fails", func() {
				// setup

				locator2rawClient.EXPECT().
					IsOpen().
					Return(true)
				locator2rawClient.EXPECT().
					DeleteStream(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string"))

				mockRawClient.EXPECT().
					DeleteStream(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string")).
					Return(errors.New("something went wrong")).
					Times(3)

				// act
				err := environment.DeleteStream(rootCtx, "retried-delete-stream")
				Expect(err).ToNot(HaveOccurred())
			})

			It("gives up on non-retryable errors", func() {
				// setup
				mockRawClient.EXPECT().
					DeleteStream(gomock.AssignableToTypeOf(ctxType), gomock.Eq("non-retryable")).
					Return(raw.ErrStreamDoesNotExist)

				// act
				err := environment.DeleteStream(rootCtx, "non-retryable")
				Expect(err).To(HaveOccurred())
			})
		})
	})

	Context("close", func() {
		// close the environment connection.
		// TODO when there are active producers/consumers we will need to stop them first
		It("closes the environment connection", func() {
			// setup
			mockRawClient.EXPECT().
				Close(gomock.AssignableToTypeOf(ctxType))

			environment.Close(rootCtx)
		})

		When("there is an error", func() {
			BeforeEach(func() {
				// setup
				mockRawClient.EXPECT().
					Close(gomock.AssignableToTypeOf(ctxType)).
					Return(errors.New("something went wrong"))
			})

			It("logs the error and moves on", func() {
				logBuffer := gbytes.NewBuffer()
				logger := slog.New(slog.NewTextHandler(logBuffer))
				ctx := raw.NewContextWithLogger(context.Background(), *logger)

				environment.Close(ctx)

				Eventually(logBuffer).WithTimeout(time.Second).Should(gbytes.Say(`"error closing locator client" close.error="something went wrong"`))
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

	Context("query stream stats", func() {
		BeforeEach(func() {
			environment.SetServerVersion("3.11.1")
			mockRawClient.EXPECT().
				IsOpen().
				Return(true) // from maybeInitializeLocator
		})

		It("queries stats for a given stream", func() {
			// setup
			mockRawClient.EXPECT().
				StreamStats(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string")).
				Return(map[string]int64{"first_chunk_id": 40, "committed_chunk_id": 42}, nil)

			// act
			stats, err := environment.QueryStreamStats(rootCtx, "stream-with-stats")
			Expect(err).ToNot(HaveOccurred())
			Expect(stats.FirstOffset()).To(BeNumerically("==", 40))
			Expect(stats.CommittedChunkId()).To(BeNumerically("==", 42))
		})

		When("there is an error", func() {
			It("bubbles up the error", func() {
				// setup
				mockRawClient.EXPECT().
					StreamStats(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string")).
					Return(nil, errors.New("err not today")).
					Times(3)

				_, err := environment.QueryStreamStats(rootCtx, "retryable-error")
				Expect(err).To(MatchError("err not today"))
			})
		})

		When("server is less than 3.11", func() {
			It("returns an unsupported error", func() {
				environment.SetServerVersion("3.10.0")
				// we do not expect any call from the Mock because it should error w/o trying

				_, err := environment.QueryStreamStats(rootCtx, "stream-with-stats")
				Expect(err).To(MatchError("unsupported operation"))
			})
		})

		When("there are multiple locators", func() {
			var (
				locator2rawClient *stream.MockRawClient
			)

			BeforeEach(func() {
				locator2rawClient = stream.NewMockRawClient(mockCtrl)
				environment.AppendLocatorRawClient(locator2rawClient)
				environment.SetBackoffPolicy(backOffPolicyFn)
				environment.SetLocatorSelectSequential(true)

				// have to set server version again because there's a new locator
				environment.SetServerVersion("3.11.1")
			})

			It("uses different locators when one fails", func() {
				// setup
				locator2rawClient.EXPECT().
					IsOpen().
					Return(true)
				locator2rawClient.EXPECT().
					StreamStats(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string")).
					Return(map[string]int64{"first_chunk_id": 40, "committed_chunk_id": 42}, nil)

				mockRawClient.EXPECT().
					StreamStats(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string")).
					Return(nil, errors.New("something went wrong")).
					Times(3)

				// act
				stats, err := environment.QueryStreamStats(rootCtx, "retried-stream-stats")
				Expect(err).ToNot(HaveOccurred())
				Expect(stats.FirstOffset()).To(BeNumerically("==", 40))
				Expect(stats.CommittedChunkId()).To(BeNumerically("==", 42))
			})

			It("gives up on non-retryable errors", func() {
				// setup
				mockRawClient.EXPECT().
					StreamStats(gomock.AssignableToTypeOf(ctxType), gomock.Eq("non-retryable")).
					Return(nil, raw.ErrStreamDoesNotExist)

				// act
				_, err := environment.QueryStreamStats(rootCtx, "non-retryable")
				Expect(err).To(HaveOccurred())
			})
		})

		It("logs intermediate error messages", func() {
			// setup
			logBuffer := gbytes.NewBuffer()
			logger := slog.New(slog.NewTextHandler(logBuffer))
			ctx := raw.NewContextWithLogger(context.Background(), *logger)

			mockRawClient.EXPECT().
				StreamStats(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string")).
				Return(nil, errors.New("err maybe later")).
				Times(3)

			// act
			_, err := environment.QueryStreamStats(ctx, "log-things")
			Expect(err).To(HaveOccurred())

			Eventually(logBuffer).Within(time.Millisecond * 500).Should(gbytes.Say(`"locator operation failed" error="err maybe later"`))
		})

	})

	Context("query offset", func() {
		BeforeEach(func() {
			mockRawClient.EXPECT().
				IsOpen().
				Return(true) // from maybeInitializeLocator
		})

		It("queries offset for a given consumer and stream", func() {
			// setup
			mockRawClient.EXPECT().
				QueryOffset(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string"), gomock.AssignableToTypeOf("string")).
				Return(uint64(42), nil)

			// act
			offset, err := environment.QueryOffset(rootCtx, "consumer-with-offset", "stream")
			Expect(err).ToNot(HaveOccurred())
			Expect(offset).To(BeNumerically("==", 42))
		})

		When("there is an error", func() {
			It("bubbles up the error", func() {
				// setup
				mockRawClient.EXPECT().
					QueryOffset(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string"), gomock.AssignableToTypeOf("string")).
					Return(uint64(0), errors.New("err not today")).
					Times(3)

				_, err := environment.QueryOffset(rootCtx, "retryable-error", "stream")
				Expect(err).To(MatchError("err not today"))
			})
		})

		When("there are multiple locators", func() {
			var (
				locator2rawClient *stream.MockRawClient
			)

			BeforeEach(func() {
				locator2rawClient = stream.NewMockRawClient(mockCtrl)
				environment.AppendLocatorRawClient(locator2rawClient)
				environment.SetBackoffPolicy(backOffPolicyFn)
				environment.SetLocatorSelectSequential(true)
			})

			It("uses different locators when one fails", func() {
				// setup
				locator2rawClient.EXPECT().
					IsOpen().
					Return(true)
				locator2rawClient.EXPECT().
					QueryOffset(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string"), gomock.AssignableToTypeOf("string")).
					Return(uint64(42), nil)

				mockRawClient.EXPECT().
					QueryOffset(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string"), gomock.AssignableToTypeOf("string")).
					Return(uint64(0), errors.New("something went wrong")).
					Times(3)

				// act
				offset, err := environment.QueryOffset(rootCtx, "retried-offset", "stream")
				Expect(err).ToNot(HaveOccurred())
				Expect(offset).To(BeNumerically("==", 42))
			})

			It("gives up on non-retryable errors", func() {
				// setup
				mockRawClient.EXPECT().
					QueryOffset(gomock.AssignableToTypeOf(ctxType), gomock.Eq("non-retryable"), gomock.AssignableToTypeOf("string")).
					Return(uint64(0), raw.ErrStreamDoesNotExist)

				// act
				_, err := environment.QueryOffset(rootCtx, "non-retryable", "stream")
				Expect(err).To(HaveOccurred())
			})
		})

		It("logs intermediate error messages", func() {
			// setup
			logBuffer := gbytes.NewBuffer()
			logger := slog.New(slog.NewTextHandler(logBuffer))
			ctx := raw.NewContextWithLogger(context.Background(), *logger)

			mockRawClient.EXPECT().
				QueryOffset(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string"), gomock.AssignableToTypeOf("string")).
				Return(uint64(0), errors.New("err maybe later")).
				Times(3)

			// act
			_, err := environment.QueryOffset(ctx, "log-things", "stream")
			Expect(err).To(HaveOccurred())

			Eventually(logBuffer).Within(time.Millisecond * 500).Should(gbytes.Say(`"locator operation failed" error="err maybe later"`))
		})

	})
	Context("query partitions", func() {
		BeforeEach(func() {
			mockRawClient.EXPECT().
				IsOpen().
				Return(true) // from maybeInitializeLocator
		})

		It("queries partition streams for a given superstream", func() {
			// setup
			mockRawClient.EXPECT().
				Partitions(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string")).
				Return([]string{"stream1, stream2"}, nil)

			// act
			partitions, err := environment.QueryPartitions(rootCtx, "superstream")
			Expect(err).ToNot(HaveOccurred())
			Expect(partitions).To(Equal([]string{"stream1, stream2"}))
		})

		When("there is an error", func() {
			It("bubbles up the error", func() {
				// setup
				mockRawClient.EXPECT().
					Partitions(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string")).
					Return(nil, errors.New("err not today")).
					Times(3)

				_, err := environment.QueryPartitions(rootCtx, "superstream-does-not-exist")
				Expect(err).To(MatchError("err not today"))
			})
		})

		When("there are multiple locators", func() {
			var (
				locator2rawClient *stream.MockRawClient
			)

			BeforeEach(func() {
				locator2rawClient = stream.NewMockRawClient(mockCtrl)
				environment.AppendLocatorRawClient(locator2rawClient)
				environment.SetBackoffPolicy(backOffPolicyFn)
				environment.SetLocatorSelectSequential(true)
			})

			It("uses different locators when one fails", func() {
				// setup
				locator2rawClient.EXPECT().
					IsOpen().
					Return(true)
				locator2rawClient.EXPECT().
					Partitions(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string")).
					Return([]string{"stream1", "stream2"}, nil)

				mockRawClient.EXPECT().
					Partitions(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string")).
					Return(nil, errors.New("something went wrong")).
					Times(3)

				// act
				partitions, err := environment.QueryPartitions(rootCtx, "superstream")
				Expect(err).ToNot(HaveOccurred())
				Expect(partitions).To(Equal([]string{"stream1", "stream2"}))
			})

			It("gives up on non-retryable errors", func() {
				// setup
				mockRawClient.EXPECT().
					Partitions(gomock.AssignableToTypeOf(ctxType), gomock.Eq("non-retryable")).
					Return(nil, raw.ErrStreamDoesNotExist)

				// act
				_, err := environment.QueryPartitions(rootCtx, "non-retryable")
				Expect(err).To(HaveOccurred())
			})
		})

		It("logs intermediate error messages", func() {
			// setup
			logBuffer := gbytes.NewBuffer()
			logger := slog.New(slog.NewTextHandler(logBuffer))
			ctx := raw.NewContextWithLogger(context.Background(), *logger)

			mockRawClient.EXPECT().
				Partitions(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string")).
				Return(nil, errors.New("err maybe later")).
				Times(3)

			// act
			_, err := environment.QueryPartitions(ctx, "log-things")
			Expect(err).To(HaveOccurred())

			Eventually(logBuffer).Within(time.Millisecond * 500).Should(gbytes.Say(`"locator operation failed" error="err maybe later"`))
		})
	})

	Context("query sequence", func() {
		BeforeEach(func() {
			mockRawClient.EXPECT().
				IsOpen().
				Return(true) // from maybeInitializeLocator
		})

		It("queries last publishingid for a given producer and stream", func() {
			// setup
			var publishingId uint64
			publishingId = 42
			mockRawClient.EXPECT().
				QueryPublisherSequence(gomock.AssignableToTypeOf(ctxType), gomock.AssignableToTypeOf("string"), gomock.AssignableToTypeOf("string")).
				Return(publishingId, nil)

			// act
			pubId, err := environment.QuerySequence(rootCtx, "producer-id", "stream-id")
			Expect(err).ToNot(HaveOccurred())
			Expect(pubId).To(BeNumerically("==", 42))
		})
	})
})
