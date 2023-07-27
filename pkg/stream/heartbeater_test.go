package stream

import (
	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"time"
)

var _ = Describe("Heartbeater", func() {

	var (
		hb            *heartBeater
		mockCtrl      *gomock.Controller
		mockRawClient *MockRawClient
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockRawClient = NewMockRawClient(mockCtrl)
		hb = NewHeartBeater(time.Millisecond*100, mockRawClient, nil)
	})

	It("can configure the tick duration", func() {
		Expect(hb.tickDuration).To(BeNumerically("==", 100000000))
	})

	It("when the tick duration expires, it sends a heartbeat", func() {
		// setup the mock
		mockRawClient.EXPECT().SendHeartbeat()
		mockRawClient.EXPECT().NotifyHeartbeat().
			DoAndReturn(func() <-chan *raw.Heartbeat {
				return make(chan *raw.Heartbeat)
			})
		hb.start()
		// wait until the mock gets called
		// the mock will fail the test in SendHeartbeat is not called
		<-time.After(time.Millisecond * 150)
	})

	It("sends a heartbeat when it receives one from the server", func(ctx SpecContext) {
		var receiveCh chan *raw.Heartbeat
		mockRawClient.EXPECT().NotifyHeartbeat().
			DoAndReturn(func() <-chan *raw.Heartbeat {
				receiveCh = make(chan *raw.Heartbeat)
				return receiveCh
			})
		mockRawClient.EXPECT().SendHeartbeat()

		hb.start()

		select {
		case <-ctx.Done():
			Fail("failed in setup: did not send a heartbeat notification")
		case receiveCh <- &raw.Heartbeat{}:
		}

		// wait until the mock gets called
		// the mock will fail the test in SendHeartbeat is not called
		<-time.After(time.Millisecond * 50)
		hb.stop()
	}, SpecTimeout(100*time.Millisecond))

	It("stops the heartbeater gracefully", func() {
		// TODO use the gleak detector
		mockRawClient.EXPECT().NotifyHeartbeat().
			DoAndReturn(func() <-chan *raw.Heartbeat {
				return make(chan *raw.Heartbeat)
			})

		hb.tickDuration = time.Minute // we do not want to receive a tick
		hb.start()
		Expect(hb.done).ToNot(BeClosed())
		Expect(hb.ticker.C).ToNot(BeClosed())

		hb.stop()
		Expect(hb.done).To(BeClosed())
		Consistently(hb.ticker.C, "100ms").ShouldNot(Receive())

		By("not panicking on subsequent close")
		// FIXME close on a closed channel panics
		hb.stop()
		// TODO investigate using gleak and asserts that heartbeater go routine have not leaked
		// 		tried this before, but could not make the test go red, even after leaking the heartbeater routine
	})
})
