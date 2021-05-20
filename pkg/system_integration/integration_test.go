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

var _ = Describe("Integration tests", func() {

	BeforeEach(func() {
		time.Sleep(200 * time.Millisecond)

	})
	AfterEach(func() {
		time.Sleep(300 * time.Millisecond)

	})

	It("Connect to all the nodes", func() {
		env, err := stream.NewEnvironment(
			stream.NewEnvironmentOptions().SetHost("node0").SetUser("test").SetPassword("test"))
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
			producer1, err := env.NewProducer(stream, nil, nil)
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
			producer1, err := env.NewProducer(stream, nil, nil)
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

})
