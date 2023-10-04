package stream

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"sync"
)

var _ = Describe("confirmation", func() {

	var (
		ctracker *confirmationTracker
	)

	BeforeEach(func() {
		ctracker = newConfirmationTracker(10)
	})

	It("adds messages and maps them by publishing ID", func() {
		m := &amqp.Message{Data: []byte("message")}

		ctracker.add(raw.NewPublishingMessage(0, m))
		ctracker.add(raw.NewPublishingMessage(1, m))
		ctracker.add(raw.NewPublishingMessage(2, m))

		Expect(ctracker.messages).To(HaveLen(3))
		Expect(ctracker.messages).To(SatisfyAll(
			HaveKey(BeNumerically("==", 0)),
			HaveKey(BeNumerically("==", 1)),
			HaveKey(BeNumerically("==", 2)),
		))
		Expect(ctracker.unconfirmedMessagesSemaphore).To(HaveLen(3))
	})

	When("multiple routines add confirmations", func() {
		It("does not race", func() {
			// this test is effective when the race detector is active
			// use ginkgo --race [...ginkgo args...]
			By("adding them one by one")
			ctracker = newConfirmationTracker(20)
			var wg sync.WaitGroup
			for i := 0; i < 20; i++ {
				wg.Add(1)
				go func(i int) {
					m := &amqp.Message{Data: []byte("message")}
					ctracker.add(raw.NewPublishingMessage(uint64(i), m))
					wg.Done()
				}(i)
			}
			wg.Wait()
			Expect(ctracker.messages).To(HaveLen(20))

			By("adding them in batches")
			ctracker = newConfirmationTracker(21)
			for i := 0; i < 20; i += 3 {
				wg.Add(1)
				go func(i int) {
					m := &amqp.Message{Data: []byte("message")}
					ctracker.addMany(
						raw.NewPublishingMessage(uint64(i), m),
						raw.NewPublishingMessage(uint64(i+1), m),
						raw.NewPublishingMessage(uint64(i+2), m),
					)
					wg.Done()
				}(i)
			}
			wg.Wait()
			Expect(ctracker.messages).To(HaveLen(21))
		})
	})

	It("adds many messages", func() {
		m := &amqp.Message{Data: []byte("amazing data")}
		ctracker.addMany()

		ctracker.addMany(raw.NewPublishingMessage(0, m))
		Expect(ctracker.messages).To(HaveLen(1))
		Expect(ctracker.unconfirmedMessagesSemaphore).To(HaveLen(1))

		ctracker.addMany(
			raw.NewPublishingMessage(5, m),
			raw.NewPublishingMessage(6, m),
			raw.NewPublishingMessage(7, m),
		)
		Expect(ctracker.messages).To(HaveLen(4))
		Expect(ctracker.messages).To(SatisfyAll(
			HaveKey(BeNumerically("==", 5)),
			HaveKey(BeNumerically("==", 6)),
			HaveKey(BeNumerically("==", 7)),
		))
		Expect(ctracker.unconfirmedMessagesSemaphore).To(HaveLen(4))
	})

	Context("confirm", func() {
		var (
			m = &amqp.Message{Data: []byte("superb message")}
		)

		It("confirms one", func() {
			ctracker.add(raw.NewPublishingMessage(6, m))

			pubMsg, err := ctracker.confirm(6)
			Expect(err).ToNot(HaveOccurred())
			Expect(pubMsg.PublishingId()).To(BeNumerically("==", 6))
			Expect(pubMsg.Message()).To(Equal(m))
			Expect(ctracker.messages).To(HaveLen(0))
			Expect(ctracker.unconfirmedMessagesSemaphore).To(HaveLen(0))
		})

		When("a message is not tracked", func() {
			It("returns an error", func() {
				ctracker.add(raw.NewPublishingMessage(1, m))

				pubMsg, err := ctracker.confirm(123)
				Expect(pubMsg).To(BeNil())
				Expect(err).To(MatchError(ContainSubstring("message confirmation not tracked")))
				Expect(ctracker.unconfirmedMessagesSemaphore).To(HaveLen(1))
			})
		})
	})
})
