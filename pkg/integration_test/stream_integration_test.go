package integration_test

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	stream "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

var _ = Describe("StreamIntegration", func() {
	Context("Issue 158", func() {
		var (
			addresses = []string{
				"rabbitmq-stream://guest:guest@localhost:5552/"}
			streamName           = fmt.Sprintf("test-next-%d", time.Now().UnixNano())
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
				for confirmations := range confirmationCh {
					for i := range confirmations {
						Expect(confirmations[i].IsConfirmed()).To(BeTrue())
					}
					for range confirmations {
						count += 1
						if count == totalExpected {
							break loop
						}
					}
				}
				c <- true
			}(readyCh)

			for i := range totalInitialMessages {
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

		It("consumes from an existing stream", Focus, func() {
			By("attaching using Next strategy")
			options := stream.NewConsumerOptions().
				SetConsumerName("golang-client-issue-158-test").
				SetOffset(stream.OffsetSpecification{}.Next()).
				SetManualCommit()

			receivedOffsets := make([]int64, 0)
			m := sync.Mutex{} // To avoid races in the handler and test assertions
			handleMessages := func(consumerContext stream.ConsumerContext, _ *amqp.Message) {
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

			// wait a bit. We don't have confirmation here

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

	Context("Initial timestamp offset when no messages exist", func() {
		var (
			addresses = []string{
				"rabbitmq-stream://guest:guest@localhost:5552/"}
			streamName = "empty-test-stream"
			streamEnv  *stream.Environment
		)

		// init empty stream
		BeforeEach(func() {
			var err error
			streamEnv, err = stream.NewEnvironment(
				stream.NewEnvironmentOptions().SetUris(addresses))
			Expect(err).ToNot(HaveOccurred())

			err = streamEnv.DeclareStream(streamName,
				stream.NewStreamOptions().SetMaxLengthBytes(stream.ByteCapacity{}.GB(2)))
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			Expect(streamEnv.DeleteStream(streamName)).
				To(SatisfyAny(
					Succeed(),
					MatchError(stream.StreamDoesNotExist),
				))
		})

		It("correctly handles offsets using timestamps when no messages exist", func() {
			var err error
			const consumerName = "timestamp-offset-consumer"

			lastMinute := time.Now().Add(-time.Minute).UnixMilli()

			// Implement the UpdateConsumer function to return a timestamp-based offset if no offset exists
			// For example, we add a new consumer to the incoming stream and don't want to reread it from the beginning.
			updateConsumer := func(streamName string, _ bool) stream.OffsetSpecification {
				offset, err := streamEnv.QueryOffset(consumerName, streamName)
				if errors.Is(err, stream.OffsetNotFoundError) {
					return stream.OffsetSpecification{}.Timestamp(lastMinute)
				}

				Expect(err).ToNot(HaveOccurred())

				return stream.OffsetSpecification{}.Offset(offset + 1)
			}

			options := stream.NewConsumerOptions().
				SetConsumerName(consumerName).
				SetAutoCommit(stream.NewAutoCommitStrategy().
					SetFlushInterval(time.Second)).
				SetSingleActiveConsumer(stream.NewSingleActiveConsumer(updateConsumer))

			// Create the consumer
			consumer, err := streamEnv.NewConsumer(
				streamName,
				func(_ stream.ConsumerContext, _ *amqp.Message) {},
				options,
			)
			Expect(err).NotTo(HaveOccurred())

			// Wait for a flush without messages
			// An incorrect offset is stored during this flush
			time.Sleep(time.Millisecond * 1200)
			Expect(consumer.Close()).ToNot(HaveOccurred())

			// Re-create the consumer
			consumeIsStarted := make(chan struct{})
			handleMessages := func(_ stream.ConsumerContext, _ *amqp.Message) {
				close(consumeIsStarted)
			}

			consumer, err = streamEnv.NewConsumer(streamName, handleMessages, options)
			Expect(err).NotTo(HaveOccurred())

			producer, err := streamEnv.NewProducer(streamName, nil)
			Expect(err).ToNot(HaveOccurred())
			body := `{"name": "item-1}`
			err = producer.Send(amqp.NewMessage([]byte(body)))
			Expect(err).ToNot(HaveOccurred())

			// check if messages are consumed
			select {
			case <-consumeIsStarted:
			case <-time.After(time.Second * 1):
				Fail("Timeout waiting for consumer to start")
			}

			Expect(consumer.GetOffset()).To(BeNumerically("<=", 0))
		})

	})
})
