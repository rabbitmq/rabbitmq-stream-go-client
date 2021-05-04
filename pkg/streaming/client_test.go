package streaming

import (
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var testEnviroment *Environment
var testStreamName string
var _ = BeforeSuite(func() {
	client, err := NewEnvironment(nil)
	testEnviroment = client
	Expect(err).NotTo(HaveOccurred())
	testStreamName = uuid.New().String()
})

var _ = AfterSuite(func() {
	testEnviroment.Close()
	time.Sleep(500 * time.Millisecond)
	Expect(testEnviroment.clientLocator.coordinator.ProducersCount()).To(Equal(0))
	Expect(testEnviroment.clientLocator.coordinator.ResponsesCount()).To(Equal(0))
	Expect(testEnviroment.clientLocator.coordinator.ConsumersCount()).To(Equal(0))
})

var _ = Describe("Streaming testEnviroment", func() {
	BeforeEach(func() {

	})
	AfterEach(func() {
	})

	It("Create Stream", func() {
		err := testEnviroment.DeclareStream(testStreamName, nil)
		Expect(err).NotTo(HaveOccurred())
		err = testEnviroment.DeleteStream(testStreamName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Create Stream with parameter MaxLengthBytes", func() {
		streamP := uuid.New().String()

		err := testEnviroment.DeclareStream(streamP, NewStreamOptions().
			MaxLengthBytes(ByteCapacity{}.MB(100)))
		Expect(err).NotTo(HaveOccurred())
		err = testEnviroment.DeleteStream(streamP)
		Expect(err).NotTo(HaveOccurred())

	})

	It("Create Stream with parameter MaxLengthBytes Error", func() {
		streamP := uuid.New().String()

		err := testEnviroment.DeclareStream(streamP, NewStreamOptions().
			MaxLengthBytes(ByteCapacity{}.From("not_a_valid_value")))

		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Invalid unit size format"))
	})

	It("Create Stream with parameter MaxAge", func() {
		streamP := uuid.New().String()
		err := testEnviroment.DeclareStream(streamP, NewStreamOptions().
			MaxAge(120*time.Hour))
		Expect(err).NotTo(HaveOccurred())
		err = testEnviroment.DeleteStream(streamP)
		Expect(err).NotTo(HaveOccurred())

	})

	It("Create two times Stream", func() {
		err := testEnviroment.DeclareStream(testStreamName, nil)
		Expect(err).NotTo(HaveOccurred())
		err = testEnviroment.DeclareStream(testStreamName, nil)
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Stream already exists"))
		err = testEnviroment.DeleteStream(testStreamName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Create two times Stream precondition fail", func() {
		err := testEnviroment.DeclareStream(testStreamName, nil)
		Expect(err).NotTo(HaveOccurred())
		err = testEnviroment.DeclareStream(testStreamName, NewStreamOptions().
			MaxLengthBytes(ByteCapacity{}.MB(100)))
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Precondition Failed"))
		err = testEnviroment.DeleteStream(testStreamName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Create empty Stream  fail", func() {
		err := testEnviroment.DeclareStream("", nil)
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("stream name can't be empty"))
	})

})
