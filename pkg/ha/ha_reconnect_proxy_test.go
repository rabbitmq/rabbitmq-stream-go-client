package ha

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	. "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	test_helper "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/test-helper"
)

// These tests simulate the network failures with a TCP proxy placed between the
// client and the broker: no HTTP management API is involved.
// The back off is 1 second so the reconnection is fast.
var _ = Describe("Reliable entities reconnection with a TCP proxy", func() {

	const reconnectionTimeout = 15 * time.Second

	var (
		proxy      *test_helper.TCPProxy
		proxyEnv   *Environment // environment used by the reliable entities
		directEnv  *Environment // environment connected directly to the broker, never dropped
		streamName string
		backOff    = &BackOffConfiguration{MinInterval: 1, MaxInterval: 1, EnableRandom: false}
	)

	BeforeEach(func() {
		var err error
		proxy, err = test_helper.NewTCPProxy("localhost:5552")
		Expect(err).NotTo(HaveOccurred())

		proxyEnv, err = NewEnvironment(NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(proxy.Port()).
			SetAddressResolver(AddressResolver{Host: "localhost", Port: proxy.Port()}))
		Expect(err).NotTo(HaveOccurred())

		directEnv, err = NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())

		streamName = uuid.New().String()
		Expect(directEnv.DeclareStream(streamName, nil)).To(Succeed())
	})

	AfterEach(func() {
		_ = proxyEnv.Close()
		proxy.Close()
		Expect(directEnv.DeleteStream(streamName)).To(Succeed())
		_ = directEnv.Close()
	})

	// newCountingProducer returns a producer, counting the confirmed and the failed messages
	newCountingProducer := func() (*ReliableProducer, *atomic.Int32, *atomic.Int32) {
		confirmed, failed := &atomic.Int32{}, &atomic.Int32{}
		producer, err := NewReliableProducerWithBackOffConfiguration(proxyEnv, streamName,
			NewProducerOptions().SetClientProvidedName(uuid.New().String()),
			func(statuses []*ConfirmationStatus) {
				for _, s := range statuses {
					if s.IsConfirmed() {
						confirmed.Add(1)
					} else {
						failed.Add(1)
					}
				}
			}, backOff)
		Expect(err).NotTo(HaveOccurred())
		return producer, confirmed, failed
	}

	sendMessages := func(producer *ReliableProducer, n int) {
		for range n {
			Expect(producer.Send(amqp.NewMessage([]byte("ha")))).To(Succeed())
		}
	}

	// messagesInStream counts the messages stored in the stream, reading with an
	// independent consumer connected directly to the broker
	messagesInStream := func(expected int) {
		var count atomic.Int32
		consumer, err := directEnv.NewConsumer(streamName,
			func(_ ConsumerContext, _ *amqp.Message) { count.Add(1) },
			NewConsumerOptions().SetOffset(OffsetSpecification{}.First()))
		Expect(err).NotTo(HaveOccurred())
		defer func() { _ = consumer.Close() }()
		Eventually(count.Load, 5*time.Second).Should(BeEquivalentTo(expected))
	}

	type scenario struct {
		name   string
		breaks func()
	}
	scenarios := func() []scenario {
		return []scenario{
			{"the connection is dropped", func() { proxy.DropConnections() }},
			{"the broker is unreachable for a while", func() {
				proxy.Outage()
				time.AfterFunc(2*time.Second, proxy.Restore)
			}},
			{"the broker sends a metadata update", func() { proxy.SendMetadataUpdate(streamName) }},
		}
	}

	Describe("Reliable Producer", func() {
		for _, sc := range scenarios() {
			It(fmt.Sprintf("reconnects and confirms all the messages when %s", sc.name), func() {
				producer, confirmed, failed := newCountingProducer()
				sendMessages(producer, 10)
				Eventually(confirmed.Load, 5*time.Second).Should(BeEquivalentTo(10))

				start := time.Now()
				sc.breaks()
				Eventually(producer.GetStatus, 2*time.Second, 10*time.Millisecond).
					Should(Equal(StatusReconnecting))

				// Send is blocked until the producer is reconnected
				sendMessages(producer, 10)
				Eventually(producer.GetStatus, reconnectionTimeout, 50*time.Millisecond).Should(Equal(StatusOpen))
				Expect(time.Since(start)).To(BeNumerically("<", 10*time.Second))

				Eventually(confirmed.Load, 5*time.Second).Should(BeEquivalentTo(20))
				Expect(failed.Load()).To(BeZero())
				Expect(producer.Close()).To(Succeed())
				messagesInStream(20)
			})
		}

		It("reconnects after repeated drops", func() {
			producer, confirmed, _ := newCountingProducer()
			for round := 1; round <= 3; round++ {
				sendMessages(producer, 5)
				proxy.DropConnections()
				Eventually(producer.GetStatus, 2*time.Second, 10*time.Millisecond).
					Should(Equal(StatusReconnecting))
				Eventually(producer.GetStatus, reconnectionTimeout, 50*time.Millisecond).Should(Equal(StatusOpen))
			}
			sendMessages(producer, 5)
			Eventually(confirmed.Load, 5*time.Second).Should(BeNumerically(">=", 5))
			Expect(producer.Close()).To(Succeed())
		})
	})

	Describe("Reliable Consumer", func() {
		for _, sc := range scenarios() {
			It(fmt.Sprintf("reconnects and resumes without loss or duplicates when %s", sc.name), func() {
				publish := func(from, to int) {
					var confirmed atomic.Int32
					producer, err := directEnv.NewProducer(streamName, nil)
					Expect(err).NotTo(HaveOccurred())
					defer func() { _ = producer.Close() }()
					go func(c ChannelPublishConfirm) {
						for statuses := range c {
							confirmed.Add(int32(len(statuses)))
						}
					}(producer.NotifyPublishConfirmation())
					for i := from; i < to; i++ {
						Expect(producer.Send(amqp.NewMessage([]byte(fmt.Sprintf("%d", i))))).To(Succeed())
					}
					Eventually(confirmed.Load, 5*time.Second).Should(BeEquivalentTo(to - from))
				}

				var mu sync.Mutex
				received := map[string]int{}
				consumer, err := NewReliableConsumerWithBackOffConfiguration(proxyEnv, streamName,
					NewConsumerOptions().SetOffset(OffsetSpecification{}.First()),
					func(_ ConsumerContext, message *amqp.Message) {
						mu.Lock()
						received[string(message.GetData())]++
						mu.Unlock()
					}, backOff)
				Expect(err).NotTo(HaveOccurred())

				count := func() int {
					mu.Lock()
					defer mu.Unlock()
					return len(received)
				}

				publish(0, 10)
				Eventually(count, 5*time.Second).Should(Equal(10))

				start := time.Now()
				sc.breaks()
				Eventually(consumer.GetStatus, 2*time.Second, 10*time.Millisecond).
					Should(Equal(StatusReconnecting))
				Eventually(consumer.GetStatus, reconnectionTimeout, 50*time.Millisecond).Should(Equal(StatusOpen))
				Expect(time.Since(start)).To(BeNumerically("<", 10*time.Second))

				publish(10, 20)
				Eventually(count, 5*time.Second).Should(Equal(20))
				mu.Lock()
				for key, times := range received {
					Expect(times).To(Equal(1), "message %s was delivered %d times", key, times)
				}
				mu.Unlock()
				Expect(consumer.Close()).To(Succeed())
			})
		}
	})
})
