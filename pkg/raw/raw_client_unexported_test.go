package raw

import (
	"bufio"
	"context"
	"github.com/go-logr/logr"
	"github.com/golang/mock/gomock"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sync"
)

type fakeCommandWrite struct {
	id uint32
}

func (f *fakeCommandWrite) Write(w *bufio.Writer) (int, error) {
	return 0, nil
}

func (f *fakeCommandWrite) Key() uint16 {
	return 42
}

func (f *fakeCommandWrite) SizeNeeded() int {
	return 123
}

func (f *fakeCommandWrite) SetCorrelationId(id uint32) {
	f.id = id
}

func (f *fakeCommandWrite) CorrelationId() uint32 {
	return f.id
}

func (f *fakeCommandWrite) Version() int16 {
	return 321
}

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

	Context("correlation map",  func() {
		var c *Client

		BeforeEach(func() {
			c = &Client{
				correlationsMap: sync.Map{},
				nextCorrelation: 9,
			}
		})

		When("a correlation exists", func() {
			It("works as expected", func() {
				By("storing the correlation")
				c.storeCorrelation(&fakeCommandWrite{})

				By("loading the correlation")
				co := c.getCorrelationById(10)
				Expect(co.id).To(BeNumerically("==", 10))

				By("removing the correlation")
				ctx := context.Background()
				c.removeCorrelation(logr.NewContext(ctx, GinkgoLogr), 10)

				By("incrementing the next correlation sequence")
				for i := 11; i < 15; i++ {
					Expect(c.getNextCorrelation()).
						To(BeNumerically("==", i), "expected sequence to equal %d", i)
				}
			})
		})

		When("a correlation does not exist", func() {
			It("fails gracefully", func() {
				Expect(func() { c.getCorrelationById(12345) }).ToNot(Panic())
				Expect(c.getCorrelationById(9876)).To(BeNil())
				ctx := context.Background()
				Expect(func() { c.removeCorrelation(logr.NewContext(ctx, GinkgoLogr), 45678) }).ToNot(Panic())
			})
		})

		When("multiple routines access the map", func() {
			It("returns unique numbers", func() {
				wg := sync.WaitGroup{}
				mu := sync.Mutex{}
				m := make(map[uint32]bool, 100)
				for i := 0; i < 100; i++ {
					wg.Add(1)
					go func() {
						defer GinkgoRecover()
						defer wg.Done()
						correlationId := c.getNextCorrelation()
						mu.Lock()
						_, found := m[correlationId]
						mu.Unlock()
						Expect(found).WithOffset(1).
							To(BeFalse(), "expected to not found correlationID %d", correlationId)

						mu.Lock()
						m[correlationId] = true
						mu.Unlock()
					}()
				}
				wg.Wait()
			})
		})
	})
})
