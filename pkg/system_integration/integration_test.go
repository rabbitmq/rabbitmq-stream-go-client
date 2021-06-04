package system_integration

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"strconv"
	"time"
)

func CreateArrayMessagesForTesting(numberOfMessages int) []*amqp.Message {
	var arr []*amqp.Message
	for z := 0; z < numberOfMessages; z++ {
		arr = append(arr, amqp.NewMessage([]byte("test_"+strconv.Itoa(z))))
	}
	return arr
}

var _ = FDescribe("Integration tests", func() {
	nodes := []string{"node0", "node1", "node2"}

	addresses := []string{
		"rabbitmq-stream://guest:guest@localhost:5552/%2f",
		"rabbitmq-stream://guest:guest@localhost:5553/%2f",
		"rabbitmq-stream://guest:guest@localhost:5554/%2f"}

	BeforeEach(func() {
		time.Sleep(200 * time.Millisecond)
		for _, node := range nodes {
			err := Operations{}.StartRabbitMQNode(node)
			Expect(err).NotTo(HaveOccurred())
		}

	})
	AfterEach(func() {
		time.Sleep(300 * time.Millisecond)

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
			_, err = producer1.BatchPublish(nil, CreateArrayMessagesForTesting(1000))
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
			_, err = producer1.BatchPublish(nil, CreateArrayMessagesForTesting(1000))
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

	FIt("HA producer test reconnection", func() {
		env, err := stream.NewEnvironment(
			stream.NewEnvironmentOptions().SetUris(addresses))
		Expect(err).NotTo(HaveOccurred())
		Expect(env).NotTo(BeNil())

		streamName := uuid.New().String()

		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())

		rProducer := stream.NewHAProducer(env)
		err = rProducer.NewProducer(streamName, "producer-ha-test")
		Expect(err).NotTo(HaveOccurred())
		Expect(rProducer.IsOpen()).To(Equal(true))

		data, err := env.StreamMetaData(streamName)
		Expect(err).NotTo(HaveOccurred())
		Expect(rProducer.GetConnectedBroker().Host).To(Equal(data.Leader.Host))
		err = Operations{}.StopRabbitMQNode(data.Leader.Host)
		Expect(err).NotTo(HaveOccurred())
		Expect(rProducer.IsOpen()).To(Equal(true))
		Expect(rProducer.GetConnectedBroker().Host).NotTo(Equal(data.Leader.Host))

		time.Sleep(1 * time.Second)

		err = Operations{}.StartRabbitMQNode(data.Leader.Host)
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

		rProducer := stream.NewHAProducer(env)
		err = rProducer.NewProducer(streamName, "producer-ha-test")
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

	FIt("Remove replica", func() {
		env, err := stream.NewEnvironment(
			stream.NewEnvironmentOptions().SetUris(addresses))
		Expect(err).NotTo(HaveOccurred())
		Expect(env).NotTo(BeNil())
		streamName := uuid.New().String()
		err = env.DeclareStream(streamName, nil)
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(5 * time.Second)
		data, err := env.StreamMetaData(streamName)
		Expect(err).NotTo(HaveOccurred())

		rProducer := stream.NewHAProducer(env)
		err = rProducer.NewProducer(streamName, "remove-replica-test")
		Expect(err).NotTo(HaveOccurred())
		Expect(rProducer.GetConnectedBroker().Host).To(Equal(data.Leader.Host))

		err = Operations{}.DeleteReplica(data.Leader.Host, streamName)
		Expect(err).NotTo(HaveOccurred())

		data1, err1 := env.StreamMetaData(streamName)
		Expect(err1).NotTo(HaveOccurred())
		Expect(rProducer.GetConnectedBroker().Host).To(Equal(data1.Leader.Host))
		Expect(data1.Leader.Host).NotTo(Equal(data.Leader.Host))

		err = env.Close()
		Expect(err).NotTo(HaveOccurred())
	})

})
