package integration_test

import (
	"fmt"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	stream "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

var _ = Describe("StreamIntegration", func() {
	Context("Issue 158", func() {
		var (
			addresses []string = []string{
				"rabbitmq-stream://guest:guest@localhost:5552/"}
			streamName           string = "test-next"
			streamEnv            *stream.Environment
			producer             *stream.Producer
			totalInitialMessages int
		)

		BeforeEach(func() {
			var err error
			streamEnv, err = stream.NewEnvironment(
				stream.NewEnvironmentOptions().SetUris(addresses))
			Expect(err).ToNot(HaveOccurred())

			err = streamEnv.DeclareStream(streamName,
				stream.NewStreamOptions().SetMaxLengthBytes(stream.ByteCapacity{}.GB(2)))
			Expect(err).ToNot(HaveOccurred())

			producer, err = streamEnv.NewProducer(streamName, nil)
			Expect(err).ToNot(HaveOccurred())
			confirmationCh := producer.NotifyPublishConfirmation()
			readyCh := make(chan bool)

			totalInitialMessages = 100
			// Routine to receive message confirmations
			// Required to ensure there are existing messages before we
			// attach to the stream
			go func(c chan bool) {
				totalExpected := totalInitialMessages
				count := 0
			loop:
				for {
					select {
					case confirmations := <-confirmationCh:
						for range confirmations {
							count += 1
							if count == totalExpected {
								break loop
							}
						}
					}
				}
				c <- true
			}(readyCh)

			for i := 0; i < totalInitialMessages; i++ {
				var message message.StreamMessage
				body := fmt.Sprintf(`{"name": "item-%d", "age": %d}`, i, i)
				message = amqp.NewMessage([]byte(body))
				err = producer.Send(message)
				Expect(err).ToNot(HaveOccurred())
			}

			// Wait for all confirmations
			<-readyCh
		})

		AfterEach(func() {
			Expect(streamEnv.DeleteStream(streamName)).
				To(SatisfyAny(
					Succeed(),
					MatchError(stream.StreamDoesNotExist),
				))
		})

		It("consumes from an existing stream", func() {
			By("attaching using Next strategy")
			options := stream.NewConsumerOptions().
				SetConsumerName("golang-client-issue-158-test").
				SetOffset(stream.OffsetSpecification{}.Next()).
				SetManualCommit()

			receivedOffsets := make([]int64, 0)
			m := sync.Mutex{} // To avoid races in the handler and test assertions
			handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
				defer GinkgoRecover()
				m.Lock()
				receivedOffsets = append(
					receivedOffsets,
					consumerContext.Consumer.GetOffset(),
				)
				m.Unlock()
				Expect(consumerContext.Consumer.StoreOffset()).To(Succeed())
			}

			consumer, err := streamEnv.NewConsumer(streamName, handleMessages, options)
			Expect(err).ToNot(HaveOccurred())

			newMessagesExpected := 100
			for i := totalInitialMessages; i < totalInitialMessages+newMessagesExpected; i++ {
				var message message.StreamMessage
				body := fmt.Sprintf(`{"name": "item-%d", "age": %d}`, i, i)
				message = amqp.NewMessage([]byte(body))
				err = producer.Send(message)
				Expect(err).ToNot(HaveOccurred())
			}

			// We should receive only 100 messages because Next sends the next chunk
			// in the stream. The previously 100 messages should be in a different chunk
			By("receiving only new messages")
			Eventually(func() int {
				m.Lock()
				defer m.Unlock()
				return len(receivedOffsets)
			}).
				WithTimeout(time.Second * 3).
				WithPolling(time.Millisecond * 500).
				Should(BeNumerically("==", 100))

			firstExpectedOffset := 100
			for i := 0; i < len(receivedOffsets); i++ {
				m.Lock()
				Expect(receivedOffsets[i]).To(BeNumerically("==", firstExpectedOffset+i),
					"Offset in [%d] is %d, expected %d",
					i, receivedOffsets[i], firstExpectedOffset+i)
				m.Unlock()
			}
			// Current offset is initial (first) + total received msg - 1
			// -1 because the first offset is 100 (it's not 101)
			// e.g. 100, 101 ... 199. NOT 200
			// Similar when 0, 1...99 (not 100)
			expectedCurrentOffset := firstExpectedOffset + newMessagesExpected - 1
			Expect(consumer.GetOffset()).To(BeNumerically("==", expectedCurrentOffset))
		})
	})
})
