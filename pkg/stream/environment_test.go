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
		mockRawClient *MockRawClient
		environment   *stream.Environment
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockRawClient = NewMockRawClient(mockCtrl)

		c := stream.NewEnvironmentConfiguration(
			stream.WithLazyInitialization(true),
			stream.WithUri("fakehost:1234"),
		)
		var err error
		environment, err = stream.NewEnvironment(c)
		Expect(err).ToNot(HaveOccurred())

		environment.SetLocatorRawClient("fakehost:1234", mockRawClient)
	})

	Context("create stream", func() {
		It("creates a stream", func() {
			// setup
			var ctx = reflect.TypeOf((*context.Context)(nil)).Elem()
			mockRawClient.EXPECT().DeclareStream(
				gomock.AssignableToTypeOf(ctx),
				gomock.Eq("my-stream"),
				gomock.AssignableToTypeOf(raw.StreamConfiguration{}),
			)

			// act
			err := environment.CreateStream(context.Background(), "my-stream", stream.StreamOptions{})

			// assert
			Expect(err).ToNot(HaveOccurred())
		})

		It("creates a stream with given parameters", func() {
			// setup
			var ctx = reflect.TypeOf((*context.Context)(nil)).Elem()
			mockRawClient.EXPECT().DeclareStream(
				gomock.AssignableToTypeOf(ctx),
				gomock.Eq("my-stream-with-options"),
				gomock.Eq(raw.StreamConfiguration{
					"x-max-age":                       "120s",
					"x-max-length-bytes":              "200000000",
					"x-stream-max-segment-size-bytes": "100000",
				}),
			)

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
			var ctx = reflect.TypeOf((*context.Context)(nil)).Elem()
			mockRawClient.EXPECT().
				DeclareStream(
					gomock.AssignableToTypeOf(ctx),
					gomock.AssignableToTypeOf("string"),
					gomock.AssignableToTypeOf(raw.StreamConfiguration{}),
				).
				Return(errors.New("something went wrong"))

			// act
			err := environment.CreateStream(context.Background(), "a-stream", stream.CreateStreamOptions{})

			// assert
			Expect(err).To(MatchError("something went wrong"))
		})

	})

	When("multiple routines use the environment",  func() {
		// FIXME(Zerpet): I'm not sure how reliable is this test
		//   it does not trigger the race detector. It makes some
		//   sense that it doesn't, since all access is read-only
		It("access the locators safely", func(ctx SpecContext) {
			// setup
			mockRawClient.EXPECT().
				DeclareStream(
					gomock.AssignableToTypeOf(ctx),
					gomock.AssignableToTypeOf("string"),
					gomock.AssignableToTypeOf(raw.StreamConfiguration{}),
				).
				Times(10)

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
