package system_integration

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"strconv"
	"sync/atomic"
	"time"
)

func getHttpPort(tcpPort string) string {
	switch tcpPort {
	case "5552":
		return "15672"
	case "5553":
		return "15673"
	case "5554":
		return "15674"
	}
	return ""
}

var _ = Describe("Integration tests", func() {
	addresses := []string{
		"rabbitmq-stream://guest:guest@localhost:5562/%2f",
		"rabbitmq-stream://guest:guest@localhost:5572/%2f",
		"rabbitmq-stream://guest:guest@localhost:5582/%2f"}

	BeforeEach(func() {
		time.Sleep(500 * time.Millisecond)

	})
	AfterEach(func() {
		time.Sleep(800 * time.Millisecond)

	})

	It("Connect to all the addresses", func() {
		env, err := stream.NewEnvironment(
			stream.NewEnvironmentOptions().SetUris(addresses))
		Expect(err).NotTo(HaveOccurred())
		Expect(env).NotTo(BeNil())

		var streams []string

		for i := 0; i < 50; i++ {
			streams = append(streams, uuid.New().String())
		}

		for _, stream := range streams {
			err = env.DeclareStream(stream, nil)
			Expect(err).NotTo(HaveOccurred())
		}

		var producers []*stream.Producer
		for _, stream := range streams {
			producer1, err := env.NewProducer(stream, nil)
			producers = append(producers, producer1)
			Expect(err).NotTo(HaveOccurred())
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(len(env.ClientCoordinator())).To(Equal(3))
		Expect(env.Nodes()).To(Equal([]string{"node0:5552", "node1:5552", "node2:5552"}))

		Expect(len(env.ClientCoordinator())).To(Equal(3))
		for _, stream := range streams {
			Expect(len(env.ProducerPerStream(stream))).ShouldNot(Equal(0))
		}

		for _, stream := range streams {
			Expect(len(env.ClientsPerStream(stream))).Should(Equal(1))
		}

		for _, stream := range streams {
			err := env.DeleteStream(stream)
			Expect(err).NotTo(HaveOccurred())
		}

		for _, stream := range streams {
			Expect(len(env.ProducerPerStream(stream))).Should(Equal(0))
		}
		time.Sleep(1 * time.Second)

		for _, stream := range streams {
			err = env.DeclareStream(stream, nil)
			Expect(err).NotTo(HaveOccurred())
		}

		producers = make([]*stream.Producer, 0)
		for _, stream := range streams {
			producer1, err := env.NewProducer(stream, nil)
			producers = append(producers, producer1)
			Expect(err).NotTo(HaveOccurred())
			Expect(err).NotTo(HaveOccurred())
		}

		for _, stream := range streams {
			Expect(len(env.ClientsPerStream(stream))).Should(Equal(1))
		}

		for _, producer := range producers {
			err := producer.Close()
			Expect(err).NotTo(HaveOccurred())

		}

		for _, stream := range streams {
			Expect(len(env.ClientsPerStream(stream))).Should(Equal(0))
		}

		for _, stream := range streams {
			err := env.DeleteStream(stream)
			Expect(err).NotTo(HaveOccurred())
		}

		time.Sleep(3 * time.Second)
		err = env.Close()
		Expect(err).NotTo(HaveOccurred())
		Expect(len(env.ClientCoordinator())).To(Equal(3))
	})

	It("HA producer test reconnection", func() {
		env, err := stream.NewEnvironment(
			stream.NewEnvironmentOptions().SetUris(addresses))
		Expect(err).NotTo(HaveOccurred())
		Expect(env).NotTo(BeNil())

		streamName := uuid.New().String()

		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		rProducer, err := ha.NewHAProducer(env, streamName, stream.NewProducerOptions().SetProducerName("p1"))
		Expect(err).NotTo(HaveOccurred())
		Expect(err).NotTo(HaveOccurred())
		Expect(rProducer.IsOpen()).To(Equal(true))

		data, err := env.StreamMetaData(streamName)
		Expect(err).NotTo(HaveOccurred())
		Expect(rProducer.GetBroker().Port).To(Equal(data.Leader.Port))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(10 * time.Second)
		Expect(rProducer.IsOpen()).To(Equal(true))
		Expect(rProducer.GetBroker().Port).NotTo(Equal(data.Leader.Port))

		time.Sleep(1 * time.Second)

		err = Operations{}.StartRabbitMQNode(data.Leader.Port)
		Expect(err).NotTo(HaveOccurred())

		err = rProducer.Close()
		Expect(err).NotTo(HaveOccurred())

		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())

		Expect(rProducer.IsOpen()).To(Equal(false))

		err = env.Close()
		Expect(err).NotTo(HaveOccurred())

	})
	It("Delete ha stream", func() {
		env, err := stream.NewEnvironment(
			stream.NewEnvironmentOptions().SetUris(addresses))
		Expect(err).NotTo(HaveOccurred())
		Expect(env).NotTo(BeNil())

		streamName := uuid.New().String()

		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		rProducer, err := ha.NewHAProducer(env, streamName, stream.NewProducerOptions().SetProducerName("producer-ha-test"))
		Expect(err).NotTo(HaveOccurred())
		Expect(err).NotTo(HaveOccurred())
		Expect(rProducer.IsOpen()).To(Equal(true))

		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())
		Expect(rProducer.IsOpen()).NotTo(Equal(true))

		err = rProducer.Close()
		Expect(err).NotTo(HaveOccurred())

		err = env.Close()
		Expect(err).NotTo(HaveOccurred())

	})

	It("Remove replica", func() {
		env, err := stream.NewEnvironment(
			stream.NewEnvironmentOptions().SetUris(addresses))
		Expect(err).NotTo(HaveOccurred())
		Expect(env).NotTo(BeNil())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(5 * time.Second)

		rProducer, err := ha.NewHAProducer(env, streamName, stream.NewProducerOptions().SetProducerName("producer-ha-test"))
		Expect(err).NotTo(HaveOccurred())
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 2; i++ {
			data, err := env.StreamMetaData(streamName)
			Expect(err).NotTo(HaveOccurred())
			Expect(rProducer.GetBroker().Port).To(Equal(data.Leader.Port))
			err = Operations{}.DeleteReplica(data.Leader.Port, streamName)
			Expect(err).NotTo(HaveOccurred())
			data1, err1 := env.StreamMetaData(streamName)
			Expect(err1).NotTo(HaveOccurred())
			Expect(rProducer.GetBroker().Port).To(Equal(data1.Leader.Port))
			Expect(data1.Leader.Port).NotTo(Equal(data.Leader.Port))
		}

		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())

		err = env.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("HA producer messages", func() {
		env, err := stream.NewEnvironment(
			stream.NewEnvironmentOptions().SetUris(addresses))
		Expect(err).NotTo(HaveOccurred())
		Expect(env).NotTo(BeNil())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		rProducer, err := ha.NewHAProducer(env, streamName, stream.NewProducerOptions().SetProducerName("producer-ha-test"))
		Expect(err).NotTo(HaveOccurred())
		chConfirm := rProducer.NotifyPublishConfirmation()
		var messagesNotConfirmed int32 = 0
		go func(ch stream.ChannelPublishConfirm) {
			for messages := range ch {
				for _, message := range messages {
					if !message.Confirmed {
						atomic.AddInt32(&messagesNotConfirmed, 1)
					}
				}
			}
		}(chConfirm)

		Expect(err).NotTo(HaveOccurred())
		Expect(rProducer.IsOpen()).To(Equal(true))
		go func(p *ha.ReliableProducer) {
			for i := 0; i < 100; i++ {
				err1 := p.Send(amqp.NewMessage([]byte("test_" + strconv.Itoa(i))))
				Expect(err1).NotTo(HaveOccurred())
				time.Sleep(100 * time.Millisecond)
			}
		}(rProducer)
		data, errs := env.StreamMetaData(streamName)
		Expect(errs).NotTo(HaveOccurred())
		Expect(rProducer.GetBroker().Port).To(Equal(data.Leader.Port))
		err1 := Operations{}.DeleteReplica(data.Leader.Port, streamName)
		Expect(err1).NotTo(HaveOccurred())
		data1, err1 := env.StreamMetaData(streamName)
		Expect(err1).NotTo(HaveOccurred())
		Expect(rProducer.GetBroker().Port).To(Equal(data1.Leader.Port))

		time.Sleep(8 * time.Second)

		var messagesCount int32 = 0
		handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
			atomic.AddInt32(&messagesCount, 1)
		}

		consumer, err2 := env.NewConsumer(streamName,
			handleMessages, stream.NewConsumerOptions().SetOffset(
				stream.OffsetSpecification{}.First()))
		Expect(err2).NotTo(HaveOccurred())
		time.Sleep(10 * time.Second)
		err = consumer.Close()
		Expect(err).NotTo(HaveOccurred())
		v := atomic.LoadInt32(&messagesCount)+atomic.LoadInt32(&messagesNotConfirmed) >= 1000
		Expect(v).To(Equal(true))
		err = env.DeleteStream(streamName)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(100 * time.Second)
		err = env.Close()
		Expect(err).NotTo(HaveOccurred())
	})

})
