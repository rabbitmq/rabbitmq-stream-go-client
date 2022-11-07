package raw

import (
	"context"
	"github.com/go-logr/logr"
	"github.com/golang/mock/gomock"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Unexported client functions", func() {
	When("a correlation is not found", func() {
		var (
			client  Client
			mockCtl *gomock.Controller
		)

		BeforeEach(func() {
			mockCtl = gomock.NewController(GinkgoT())
		})

		It("discards the response", func(ctx context.Context) {
			client = Client{}
			pp := internal.NewPeerPropertiesResponseWith(5, 1, map[string]string{"some": "prop"})

			fakeLogSink := NewMockLogSink(mockCtl)
			fakeLogSink.EXPECT().
				Init(gomock.Any()).
				AnyTimes()
			fakeLogSink.EXPECT().
				Enabled(gomock.Any()).
				Return(true).
				AnyTimes()

			By("logging a not-found message")
			fakeLogSink.EXPECT().
				Info(gomock.Eq(1), gomock.Eq("no correlation found for response"), gomock.Eq("response correlation"), gomock.AssignableToTypeOf(uint32(0)))
			logger := logr.New(fakeLogSink)
			loggerCtx := logr.NewContext(ctx, logger)

			By("not packing")
			Expect(func() { client.handleResponse(loggerCtx, pp) }).ToNot(Panic())
		})
	})
})
