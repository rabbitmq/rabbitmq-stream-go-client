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
	It("consumes from an existing stream", func() {
		// Setup
		addresses := []string{
			"rabbitmq-stream://guest:guest@localhost:5552/"}

		streamEnv, err := stream.NewEnvironment(
			stream.NewEnvironmentOptions().SetUris(addresses))
		Expect(err).ToNot(HaveOccurred())

		streamName := "test-next"
		err = streamEnv.DeleteStream(streamName)
		Expect(err).To(SatisfyAny(
			Not(HaveOccurred()),
			MatchError(stream.StreamDoesNotExist),
		))

		err = streamEnv.DeclareStream(streamName,
			stream.NewStreamOptions().SetMaxLengthBytes(stream.ByteCapacity{}.GB(2)))
		Expect(err).ToNot(HaveOccurred())

		producer, err := streamEnv.NewProducer(streamName, nil)
		Expect(err).ToNot(HaveOccurred())
		confirmationCh := producer.NotifyPublishConfirmation()
		readyCh := make(chan bool)

		count := 100
		// Routine to receive message confirmations
		// Required to ensure there are existing messages before we
		// attach to the stream
		go func(c chan bool) {
			totalExpected := count
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

		for i := 0; i < count; i++ {
			var message message.StreamMessage
			body := fmt.Sprintf(`{"name": "item-%d", "age": %d}`, i, i)
			message = amqp.NewMessage([]byte(body))
			err = producer.Send(message)
			Expect(err).ToNot(HaveOccurred())
		}

		// Wait for all confirmations
		<-readyCh

		// Setup consumer
		options := stream.NewConsumerOptions().
			SetConsumerName("golang-client").
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

		// Test
		for i := count; i < count*2; i++ {
			var message message.StreamMessage
			body := fmt.Sprintf(`{"name": "item-%d", "age": %d}`, i, i)
			message = amqp.NewMessage([]byte(body))
			err = producer.Send(message)
			Expect(err).ToNot(HaveOccurred())
		}

		// We should receive only 100 messages because Next sends the next chunk
		// in the stream. The previously 100 messages should be in a different chunk
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
		expectedCurrentOffset := firstExpectedOffset + count - 1
		Expect(consumer.GetOffset()).To(BeNumerically("==", expectedCurrentOffset))

		// Cleanup
		// TODO move setup to beforeEach and cleanup to afterEach
	})
})
