package stream

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

type testSuperStreamOption struct {
	partition  []string
	bindingKey []string
	args       map[string]string
}

func newTestSuperStreamOption(partition []string, bindingKey []string, args map[string]string) *testSuperStreamOption {
	return &testSuperStreamOption{
		partition:  partition,
		bindingKey: bindingKey,
		args:       args,
	}
}

func (t *testSuperStreamOption) getPartitions(_ string) []string {
	return t.partition
}

func (t *testSuperStreamOption) getBindingKeys() []string {
	return t.bindingKey
}

func (t *testSuperStreamOption) getArgs() map[string]string {
	return t.args
}

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
		err = client.DeclareSuperStream("", nil)
		Expect(err).To(HaveOccurred())

		// empty name with spaces
		err = client.DeclareSuperStream("  ", nil)
		Expect(err).To(HaveOccurred())

		// partition nil and empty
		err = client.DeclareSuperStream("valid name", nil)
		Expect(err).To(HaveOccurred())

		// bindingskeys nil and empty
		err = client.DeclareSuperStream("valid name",
			newTestSuperStreamOption([]string{"some name"}, nil, nil))
		Expect(err).To(HaveOccurred())

		// partition  empty
		err = client.DeclareSuperStream("valid name", newTestSuperStreamOption([]string{""}, []string{"some key"}, nil))
		Expect(err).To(HaveOccurred())

		// partition  key empty
		err = client.DeclareSuperStream("valid name", newTestSuperStreamOption([]string{" "}, []string{"some key"}, nil))
		Expect(err).To(HaveOccurred())

		// bindigs  key empty
		err = client.DeclareSuperStream("valid name", newTestSuperStreamOption([]string{"valid "}, []string{""}, nil))
		Expect(err).To(HaveOccurred())

		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("Create Super stream two times and delete it with client", Label("super-stream"), func() {
		client, err := testEnvironment.newReconnectClient()
		Expect(err).NotTo(HaveOccurred())

		err = client.DeclareSuperStream("go-my_super_stream_with_2_partitions", newTestSuperStreamOption([]string{"go-partition_0", "go-partition_1"}, []string{"0", "1"}, map[string]string{"queue-leader-locator": "least-leaders"}))
		Expect(err).NotTo(HaveOccurred())

		err = client.DeclareSuperStream("go-my_super_stream_with_2_partitions",
			newTestSuperStreamOption([]string{"go-partition_0", "go-partition_1"}, []string{"0", "1"}, map[string]string{"queue-leader-locator": "least-leaders"}))

		Expect(err).To(Equal(StreamAlreadyExists))

		err = client.DeleteSuperStream("go-my_super_stream_with_2_partitions")
		Expect(err).NotTo(HaveOccurred())

		err = client.DeleteSuperStream("go-my_super_stream_with_2_partitions")
		Expect(err).To(Equal(StreamDoesNotExist))

		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("Create Super stream with 3 Partitions and delete it with env", Label("super-stream"), func() {

		err := testEnvironment.DeclareSuperStream("go-my_super_stream_with_3_partitions", NewPartitionSuperStreamOptions(3))
		Expect(err).NotTo(HaveOccurred())

		err = testEnvironment.DeleteSuperStream("go-my_super_stream_with_3_partitions")
		Expect(err).NotTo(HaveOccurred())
	})

	It("Create Super stream with 2 partitions and other parameters", Label("super-stream"), func() {
		err := testEnvironment.DeclareSuperStream("go-my_super_stream_with_2_partitions_and_parameters",
			NewPartitionSuperStreamOptions(2).
				SetMaxAge(10*time.Second).
				SetMaxLengthBytes(ByteCapacity{}.GB(1)).
				SetMaxSegmentSizeBytes(ByteCapacity{}.KB(10)).
				SetLeaderLocator("least-leaders"))
		Expect(err).NotTo(HaveOccurred())

		//errWithDifferentParameters := testEnvironment.DeclareSuperStream("go-my_super_stream_with_2_partitions_and_parameters",
		//	NewPartitionSuperStreamOptions(2).
		//		SetMaxAge(30*time.Second).
		//		SetMaxLengthBytes(ByteCapacity{}.GB(10)).
		//		SetMaxSegmentSizeBytes(ByteCapacity{}.KB(100)).
		//		SetLeaderLocator("least-leaders"))
		//Expect(errWithDifferentParameters).To(HaveOccurred())

		err = testEnvironment.DeleteSuperStream("go-my_super_stream_with_2_partitions_and_parameters")
		Expect(err).NotTo(HaveOccurred())

	})

})
