package ha

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	. "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"sync/atomic"
)

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
		Expect(envForRProducer.DeleteStream(streamForRProducer)).NotTo(HaveOccurred())
	})

	It("Validate confirm handler", func() {
		_, err := NewReliableProducer(envForRProducer,
			streamForRProducer, &ProducerOptions{}, nil)
		Expect(err).To(HaveOccurred())
	})

	It("Create/Confirm and close a Reliable Producer", func() {
		signal := make(chan struct{})
		var confirmed int32
		producer, err := NewReliableProducer(envForRProducer,
			streamForRProducer, NewProducerOptions(), func(messageConfirm []*ConfirmationStatus) {
				for _, confirm := range messageConfirm {
					Expect(confirm.IsConfirmed()).To(BeTrue())
				}
				if atomic.AddInt32(&confirmed, int32(len(messageConfirm))) == 10 {
					signal <- struct{}{}
				}
			})
		Expect(err).NotTo(HaveOccurred())
		for i := 0; i < 10; i++ {
			msg := amqp.NewMessage([]byte("ha"))
			err := producer.Send(msg)
			Expect(err).NotTo(HaveOccurred())
		}
		<-signal
		Expect(producer.Close()).NotTo(HaveOccurred())
	})

	//TODO: The test is commented out because it is not possible to kill the connection from the client side
	// the client provider name is not exposed to the user.
	// we need to expose it than kill the connection

	//It("restart Reliable Producer in case of killing connection", func() {
	//	signal := make(chan struct{})
	//	var confirmed int32
	//	producer, err := NewReliableProducer(envForRProducer,
	//		streamForRProducer, &ProducerOptions{}, func(messageConfirm []*ConfirmationStatus) {
	//			for _, confirm := range messageConfirm {
	//				Expect(confirm.IsConfirmed()).To(BeTrue())
	//			}
	//			if atomic.AddInt32(&confirmed, int32(len(messageConfirm))) == 10 {
	//				signal <- struct{}{}
	//			}
	//		})
	//	Expect(err).NotTo(HaveOccurred())
	//
	//	// kill the connection
	//
	//	for i := 0; i < 10; i++ {
	//		msg := amqp.NewMessage([]byte("ha"))
	//		err := producer.Send(msg)
	//		Expect(err).NotTo(HaveOccurred())
	//	}
	//	<-signal
	//	Expect(producer.Close()).NotTo(HaveOccurred())
	//})

})
