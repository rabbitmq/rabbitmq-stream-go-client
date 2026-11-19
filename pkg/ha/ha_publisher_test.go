package ha

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	. "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	test_helper "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/test-helper"
)

// closeWhileReconnecting drops the connection named clientProvidedName, closes
// the reliable entity once it is reconnecting, and verifies that it stays closed
// and never opens a new connection. The observation outlasts the longest first
// retry backoff (11 seconds), after which an unstopped reconnection would reopen.
func closeWhileReconnecting(clientProvidedName string, getStatus func() int, closeReliable func() error) {
	Eventually(func() (bool, error) { return test_helper.IsConnectionAlive(clientProvidedName, "15672") }, 10*time.Second).
		WithPolling(500*time.Millisecond).Should(BeTrue(), "check if the connection is alive")
	Expect(test_helper.DropConnectionClientProvidedName(clientProvidedName, "15672")).To(Succeed())

	Eventually(getStatus).WithTimeout(10 * time.Second).WithPolling(10 * time.Millisecond).
		Should(Equal(StatusReconnecting))
	Expect(closeReliable()).To(Succeed(), "Close while reconnecting must succeed")
	Expect(getStatus()).To(Equal(StatusClosed))

	Eventually(func() (bool, error) { return test_helper.IsConnectionAlive(clientProvidedName, "15672") }, 5*time.Second).
		WithPolling(250*time.Millisecond).Should(BeFalse(), "the dropped connection is still listed")
	Consistently(func(g Gomega) {
		g.Expect(getStatus()).To(Equal(StatusClosed))
		alive, err := test_helper.IsConnectionAlive(clientProvidedName, "15672")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(alive).To(BeFalse(), "a connection was reopened after Close")
	}).WithTimeout(12 * time.Second).WithPolling(500 * time.Millisecond).Should(Succeed())

	Expect(closeReliable()).To(Succeed(), "Close must be idempotent")
}

var _ = Describe("Reliable Producer", func() {

	var (
		envForRProducer    *Environment
		streamForRProducer string
	)
	BeforeEach(func() {
		testEnv, err := NewEnvironment(nil)
		envForRProducer = testEnv
		Expect(err).NotTo(HaveOccurred())
		streamForRProducer = uuid.New().String()
		err = envForRProducer.DeclareStream(streamForRProducer, nil)
		Expect(err).NotTo(HaveOccurred())
	})
	AfterEach(func() {
		exists, err := envForRProducer.StreamExists(streamForRProducer)
		Expect(err).NotTo(HaveOccurred())
		if exists {
			Expect(envForRProducer.DeleteStream(streamForRProducer)).NotTo(HaveOccurred())
		}
	})

	It("Validate mandatory  fields", func() {
		_, err := NewReliableProducer(envForRProducer,
			streamForRProducer, &ProducerOptions{}, nil)
		Expect(err).To(HaveOccurred())
		_, err = NewReliableProducer(envForRProducer, streamForRProducer, nil, func(_ []*ConfirmationStatus) {})
		Expect(err).To(HaveOccurred())
	})

	It("Create/Confirm and close a Reliable Producer", func() {
		const expectedMessages = 20
		signal := make(chan struct{})
		var confirmed int32
		producer, err := NewReliableProducer(envForRProducer,
			streamForRProducer, NewProducerOptions(), func(messageConfirm []*ConfirmationStatus) {
				for _, confirm := range messageConfirm {
					Expect(confirm.IsConfirmed()).To(BeTrue())
				}
				if atomic.AddInt32(&confirmed, int32(len(messageConfirm))) == expectedMessages {
					signal <- struct{}{}
				}
			})
		Expect(err).NotTo(HaveOccurred())
		for range 10 {
			msg := amqp.NewMessage([]byte("ha"))
			err := producer.Send(msg)
			Expect(err).NotTo(HaveOccurred())
		}

		for i := 0; i < 5; i++ {
			msg1 := amqp.NewMessage([]byte("ha_batch_1"))
			msg2 := amqp.NewMessage([]byte("ha_batch_2"))
			batch := []message.StreamMessage{msg1, msg2}
			err := producer.BatchSend(batch)
			Expect(err).NotTo(HaveOccurred())
		}
		<-signal
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("restart Reliable Producer in case of killing connection", func() {
		signal := make(chan struct{})
		var confirmed int32
		clientProvidedName := uuid.New().String()
		producer, err := NewReliableProducer(envForRProducer,
			streamForRProducer, NewProducerOptions().SetClientProvidedName(clientProvidedName), func(messageConfirm []*ConfirmationStatus) {

				defer GinkgoRecover()
				for _, confirm := range messageConfirm {
					fmt.Printf("message result: %v  \n  ", confirm.IsConfirmed())
					Expect(confirm.IsConfirmed()).To(BeTrue())
				}
				if atomic.AddInt32(&confirmed, int32(len(messageConfirm))) == 10 {
					signal <- struct{}{}
				}
			})
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(1 * time.Second)
		connectionToDrop := ""
		Eventually(func() bool {
			connections, err := test_helper.Connections("15672")
			if err != nil {
				return false
			}
			for _, connection := range connections {
				if connection.ClientProperties.Connection_name == clientProvidedName {
					connectionToDrop = connection.Name
					return true
				}
			}
			return false
		}, time.Second*5).
			Should(BeTrue())

		Expect(connectionToDrop).NotTo(BeEmpty())
		// kill the connection
		errDrop := test_helper.DropConnection(connectionToDrop, "15672")
		Expect(errDrop).NotTo(HaveOccurred())

		time.Sleep(2 * time.Second) // we give some time to the client to reconnect
		for i := 0; i < 10; i++ {
			msg := amqp.NewMessage([]byte("ha"))
			err := producer.Send(msg)
			Expect(err).NotTo(HaveOccurred())
		}
		<-signal
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("unblock all Reliable Producer sends while restarting with concurrent writes", func() {
		const expectedMessages = 2
		signal := make(chan struct{})
		var confirmed int32
		clientProvidedName := uuid.New().String()
		producer, err := NewReliableProducer(envForRProducer,
			streamForRProducer,
			NewProducerOptions().
				SetClientProvidedName(clientProvidedName),
			func(messageConfirm []*ConfirmationStatus) {
				for _, confirm := range messageConfirm {
					Expect(confirm.IsConfirmed()).To(BeTrue())
				}
				if atomic.AddInt32(&confirmed, int32(len(messageConfirm))) == expectedMessages {
					signal <- struct{}{}
				}
			})
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(1 * time.Second)
		connectionToDrop := ""
		Eventually(func() bool {
			connections, err := test_helper.Connections("15672")
			if err != nil {
				return false
			}
			for _, connection := range connections {
				if connection.ClientProperties.Connection_name == clientProvidedName {
					connectionToDrop = connection.Name
					return true
				}
			}
			return false
		}, time.Second*5).
			Should(BeTrue())

		Expect(connectionToDrop).NotTo(BeEmpty())

		// concurrent writes while reconnecting
		sendMsg := func() {
			msg := amqp.NewMessage([]byte("ha"))
			batch := []message.StreamMessage{msg}
			err := producer.BatchSend(batch)
			Expect(err).NotTo(HaveOccurred())
		}

		// kill the connection
		errDrop := test_helper.DropConnection(connectionToDrop, "15672")
		Expect(errDrop).NotTo(HaveOccurred())

		time.Sleep(1 * time.Second)
		// wait for the producer to be in reconnecting state
		Eventually(func() bool {
			return producer.GetStatus() == StatusReconnecting
		}).WithPolling(300 * time.Millisecond).WithTimeout(20 * time.Second).Should(BeTrue())

		// wait for the producer to be in reconnecting state
		Eventually(func() bool {
			return producer.GetStatus() == StatusReconnecting
		}, time.Second*5, time.Millisecond).
			Should(BeTrue())

		go sendMsg()
		go sendMsg()

		<-signal
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	It("stays closed when closed while reconnecting", func() {
		clientProvidedName := uuid.New().String()
		producer, err := NewReliableProducer(envForRProducer,
			streamForRProducer, NewProducerOptions().SetClientProvidedName(clientProvidedName), func(_ []*ConfirmationStatus) {
			})
		Expect(err).NotTo(HaveOccurred())
		closeWhileReconnecting(clientProvidedName, producer.GetStatus, producer.Close)
	})

	It("Delete the stream should close the producer", func() {
		producer, err := NewReliableProducer(envForRProducer,
			streamForRProducer, NewProducerOptions(), func(_ []*ConfirmationStatus) {
			})
		Expect(err).NotTo(HaveOccurred())
		Expect(producer).NotTo(BeNil())
		Expect(envForRProducer.DeleteStream(streamForRProducer)).NotTo(HaveOccurred())
		Eventually(func() int {
			return producer.GetStatus()
		}).WithPolling(300 * time.Millisecond).WithTimeout(20 * time.Second).Should(Equal(StatusClosed))

	})

})
