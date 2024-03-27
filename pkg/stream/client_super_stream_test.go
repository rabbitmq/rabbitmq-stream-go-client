package stream

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Super Stream Client", Label("super-stream"), func() {

	var (
		testEnvironment    *Environment
		testProducerStream string
	)

	BeforeEach(func() {
		client, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		testEnvironment = client
		testProducerStream = uuid.New().String()
		Expect(testEnvironment.DeclareStream(testProducerStream, nil)).
			NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(testEnvironment.DeleteStream(testProducerStream)).NotTo(HaveOccurred())
		Expect(testEnvironment.Close()).To(Succeed())
		Eventually(testEnvironment.IsClosed, time.Millisecond*300).Should(BeTrue(), "Expected testEnvironment to be closed")
	})

	It("Validate Super Stream Creation", Label("super-stream"), func() {
		client, err := testEnvironment.newReconnectClient()
		Expect(err).NotTo(HaveOccurred())

		// empty name
		err = client.DeclareSuperStream("", nil, nil, nil)
		Expect(err).To(HaveOccurred())

		// empty name with spaces
		err = client.DeclareSuperStream("  ", nil, nil, nil)
		Expect(err).To(HaveOccurred())

		// partition nil and empty
		err = client.DeclareSuperStream("valid name", nil, nil, nil)
		Expect(err).To(HaveOccurred())

		// bindingskeys nil and empty
		err = client.DeclareSuperStream("valid name", []string{"some name"}, nil, nil)
		Expect(err).To(HaveOccurred())

		// partition  empty
		err = client.DeclareSuperStream("valid name", []string{}, []string{"some key"}, nil)
		Expect(err).To(HaveOccurred())

		// partition  key empty
		err = client.DeclareSuperStream("valid name", []string{" "}, []string{"some key"}, nil)
		Expect(err).To(HaveOccurred())

		// bindigs  key empty
		err = client.DeclareSuperStream("valid name", []string{"valid "}, []string{""}, nil)
		Expect(err).To(HaveOccurred())

		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("Create Super stream two times and delete it", Label("super-stream"), func() {
		client, err := testEnvironment.newReconnectClient()
		Expect(err).NotTo(HaveOccurred())

		err = client.DeclareSuperStream("go-my_super_stream_with_2_partitions", []string{"go-partition_0", "go-partition_1"},
			[]string{"0", "1"}, map[string]string{"queue-leader-locator": "least-leaders"})
		Expect(err).NotTo(HaveOccurred())

		err = client.DeclareSuperStream("go-my_super_stream_with_2_partitions", []string{"go-partition_0", "go-partition_1"},
			[]string{"0", "1"}, map[string]string{"queue-leader-locator": "least-leaders"})
		Expect(err).To(Equal(StreamAlreadyExists))

		err = client.DeleteSuperStream("go-my_super_stream_with_2_partitions")
		Expect(err).NotTo(HaveOccurred())

		err = client.DeleteSuperStream("go-my_super_stream_with_2_partitions")
		Expect(err).To(Equal(StreamDoesNotExist))

		Expect(client.Close()).NotTo(HaveOccurred())
	})

})
