package stream

import (
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
		testEnvironment *Environment
	)

	BeforeEach(func() {
		client, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		testEnvironment = client
	})

	AfterEach(func() {
		Expect(testEnvironment.Close()).To(Succeed())
		Eventually(testEnvironment.IsClosed, time.Millisecond*300).Should(BeTrue(), "Expected testEnvironment to be closed")
	})

	It("Validate Super Stream Creation", Label("super-stream"), func() {
		err := testEnvironment.maybeReconnectLocator()
		Expect(err).NotTo(HaveOccurred())

		// empty name
		err = testEnvironment.locator.client.DeclareSuperStream("", nil)
		Expect(err).To(HaveOccurred())

		// empty name with spaces
		err = testEnvironment.locator.client.DeclareSuperStream("  ", nil)
		Expect(err).To(HaveOccurred())

		// partition nil and empty
		err = testEnvironment.locator.client.DeclareSuperStream("valid name", nil)
		Expect(err).To(HaveOccurred())

		// bindingskeys nil and empty
		err = testEnvironment.locator.client.DeclareSuperStream("valid name",
			newTestSuperStreamOption([]string{"some name"}, nil, nil))
		Expect(err).To(HaveOccurred())

		// partition  empty
		err = testEnvironment.locator.client.DeclareSuperStream("valid name", newTestSuperStreamOption([]string{""}, []string{"some key"}, nil))
		Expect(err).To(HaveOccurred())

		// partition  key empty
		err = testEnvironment.locator.client.DeclareSuperStream("valid name", newTestSuperStreamOption([]string{" "}, []string{"some key"}, nil))
		Expect(err).To(HaveOccurred())

		// bindigs  key empty
		err = testEnvironment.locator.client.DeclareSuperStream("valid name", newTestSuperStreamOption([]string{"valid "}, []string{""}, nil))
		Expect(err).To(HaveOccurred())

		Expect(testEnvironment.locator.client.Close()).NotTo(HaveOccurred())
	})

	It("Create Super stream two times and delete it with client", Label("super-stream"), func() {
		err := testEnvironment.maybeReconnectLocator()
		Expect(err).NotTo(HaveOccurred())

		err = testEnvironment.locator.client.DeclareSuperStream("go-my_super_stream_with_2_partitions", newTestSuperStreamOption([]string{"go-partition_0", "go-partition_1"}, []string{"0", "1"}, map[string]string{"queue-leader-locator": "least-leaders"}))
		Expect(err).NotTo(HaveOccurred())

		err2 := testEnvironment.locator.client.DeclareSuperStream("go-my_super_stream_with_2_partitions",
			newTestSuperStreamOption([]string{"go-partition_0", "go-partition_1"}, []string{"0", "1"}, map[string]string{"queue-leader-locator": "least-leaders"}))

		Expect(err2).To(Equal(StreamAlreadyExists))

		err = testEnvironment.DeleteSuperStream("go-my_super_stream_with_2_partitions")
		Expect(err).NotTo(HaveOccurred())

		err = testEnvironment.DeleteSuperStream("go-my_super_stream_with_2_partitions")
		Expect(err).To(Equal(StreamDoesNotExist))

		Expect(testEnvironment.Close()).NotTo(HaveOccurred())
	})

	It("Query Partitions With client/environment", Label("super-stream"), func() {
		err := testEnvironment.maybeReconnectLocator()
		Expect(err).NotTo(HaveOccurred())

		err = testEnvironment.DeclareSuperStream("go-my_super_stream_with_query_partitions",
			NewPartitionsOptions(3))
		Expect(err).NotTo(HaveOccurred())

		partitions, err := testEnvironment.QueryPartitions("go-my_super_stream_with_query_partitions")
		Expect(err).NotTo(HaveOccurred())
		Expect(partitions).To(HaveLen(3))

		for _, partition := range partitions {
			Expect(partition).To(MatchRegexp("go-my_super_stream_with_query_partitions-\\d"))
		}

		partitionsEnv, err := testEnvironment.QueryPartitions("go-my_super_stream_with_query_partitions")
		Expect(err).NotTo(HaveOccurred())
		Expect(partitions).To(HaveLen(3))

		for _, partition := range partitionsEnv {
			Expect(partition).To(MatchRegexp("go-my_super_stream_with_query_partitions-\\d"))
		}

		Expect(testEnvironment.DeleteSuperStream("go-my_super_stream_with_query_partitions")).NotTo(HaveOccurred())
		Expect(testEnvironment.Close()).NotTo(HaveOccurred())

	})

	It("Create Super stream with 3 Partitions and delete it with env", Label("super-stream"), func() {

		err := testEnvironment.DeclareSuperStream("go-my_super_stream_with_3_partitions",
			NewPartitionsOptions(3))
		Expect(err).NotTo(HaveOccurred())

		err = testEnvironment.DeleteSuperStream("go-my_super_stream_with_3_partitions")
		Expect(err).NotTo(HaveOccurred())
	})

	It("Create Super stream with 2 partitions and other parameters", Label("super-stream"), func() {
		// Test the creation of a super stream with 2 partitions and other parameters
		// The declaration in this level is idempotent
		// so declaring the same stream with the same parameters will not return an error

		err := testEnvironment.DeclareSuperStream("go-my_super_stream_with_2_partitions_and_parameters",
			NewPartitionsOptions(2).
				SetMaxAge(24*120*time.Hour).
				SetMaxLengthBytes(ByteCapacity{}.GB(1)).
				SetMaxSegmentSizeBytes(ByteCapacity{}.KB(10)).
				SetBalancedLeaderLocator())
		Expect(err).NotTo(HaveOccurred())

		// In this case we ignore that the stream already exists
		err = testEnvironment.DeclareSuperStream("go-my_super_stream_with_2_partitions_and_parameters",
			NewPartitionsOptions(2).
				SetMaxAge(24*120*time.Hour).
				SetMaxLengthBytes(ByteCapacity{}.GB(1)).
				SetMaxSegmentSizeBytes(ByteCapacity{}.KB(10)).
				SetBalancedLeaderLocator())
		Expect(err).NotTo(HaveOccurred())

		// Delete the stream. Ok
		err = testEnvironment.DeleteSuperStream("go-my_super_stream_with_2_partitions_and_parameters")
		Expect(err).NotTo(HaveOccurred())

		// Delete a not existing stream will return an error
		err = testEnvironment.DeleteSuperStream("go-my_super_stream_with_2_partitions_and_parameters")
		Expect(err).To(Equal(StreamDoesNotExist))
	})

	It("Create Super stream with 3 keys and other parameters", Label("super-stream"), func() {
		err := testEnvironment.DeclareSuperStream("go-countries",
			NewBindingsOptions([]string{"italy", "spain", "france"}).
				SetMaxAge(24*120*time.Hour).
				SetMaxLengthBytes(ByteCapacity{}.GB(1)).
				SetMaxSegmentSizeBytes(ByteCapacity{}.KB(10)).
				SetBalancedLeaderLocator())
		Expect(err).NotTo(HaveOccurred())

		// In this case we ignore that the stream already exists
		err = testEnvironment.DeclareSuperStream("go-countries",
			NewBindingsOptions([]string{"italy", "spain", "france"}).
				SetMaxAge(24*120*time.Hour).
				SetMaxLengthBytes(ByteCapacity{}.GB(1)).
				SetMaxSegmentSizeBytes(ByteCapacity{}.KB(10)).
				SetClientLocalLocator())
		Expect(err).NotTo(HaveOccurred())

		err = testEnvironment.DeleteSuperStream("go-countries")
		Expect(err).NotTo(HaveOccurred())

		err = testEnvironment.DeleteSuperStream("go-countries")
		Expect(err).To(Equal(StreamDoesNotExist))
	})

})
