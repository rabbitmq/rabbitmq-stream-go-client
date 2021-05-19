package stream

import (
	"fmt"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var testEnvironment *Environment
var testStreamName string

var _ = BeforeSuite(func() {
	SetLevelInfo(DEBUG)
	client, err := NewEnvironment(nil)
	testEnvironment = client
	Expect(err).NotTo(HaveOccurred())
	testStreamName = uuid.New().String()
})

var _ = AfterSuite(func() {
	err := testEnvironment.Close()
	Expect(err).NotTo(HaveOccurred())
	time.Sleep(500 * time.Millisecond)
	//Expect(testEnvironment.clientLocator.coordinator.ProducersCount()).To(Equal(0))
	//Expect(testEnvironment.clientLocator.coordinator.ResponsesCount()).To(Equal(0))
	//Expect(testEnvironment.clientLocator.coordinator.ConsumersCount()).To(Equal(0))
})

var _ = Describe("Streaming testEnvironment", func() {
	BeforeEach(func() {
		time.Sleep(200 * time.Millisecond)
	})
	AfterEach(func() {
		time.Sleep(200 * time.Millisecond)
	})

	It("Create Stream", func() {
		err := testEnvironment.DeclareStream(testStreamName, nil)
		Expect(err).NotTo(HaveOccurred())
		err = testEnvironment.DeleteStream(testStreamName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Create Stream with parameter SetMaxLengthBytes", func() {
		streamP := uuid.New().String()

		err := testEnvironment.DeclareStream(streamP,
			&StreamOptions{
				MaxLengthBytes: ByteCapacity{}.GB(2),
			},
		)
		Expect(err).NotTo(HaveOccurred())
		err = testEnvironment.DeleteStream(streamP)
		Expect(err).NotTo(HaveOccurred())

	})

	It("Create Stream with parameter SetMaxLengthBytes Error", func() {
		streamP := uuid.New().String()

		err := testEnvironment.DeclareStream(streamP,
			&StreamOptions{
				MaxLengthBytes: ByteCapacity{}.From("not_a_valid_value"),
			})

		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Invalid unit size format"))
	})

	It("Create Stream with parameter SetMaxSegmentSizeBytes", func() {
		streamP := uuid.New().String()

		err := testEnvironment.DeclareStream(streamP,
			&StreamOptions{
				MaxSegmentSizeBytes: ByteCapacity{}.MB(100),
			},
		)
		Expect(err).NotTo(HaveOccurred())
		err = testEnvironment.DeleteStream(streamP)
		Expect(err).NotTo(HaveOccurred())

	})

	It("Create Stream with parameter SetMaxSegmentSizeBytes Error", func() {
		streamP := uuid.New().String()

		err := testEnvironment.DeclareStream(streamP,
			&StreamOptions{
				MaxSegmentSizeBytes: ByteCapacity{}.From("not_a_valid_value"),
			})

		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Invalid unit size format"))
	})

	It("Create Stream with parameter SetMaxAge", func() {
		streamP := uuid.New().String()
		err := testEnvironment.DeclareStream(streamP,
			&StreamOptions{
				MaxAge: 120 * time.Hour,
			})
		Expect(err).NotTo(HaveOccurred())
		err = testEnvironment.DeleteStream(streamP)
		Expect(err).NotTo(HaveOccurred())

	})

	It("Create two times Stream", func() {
		err := testEnvironment.DeclareStream(testStreamName, nil)
		Expect(err).NotTo(HaveOccurred())
		err = testEnvironment.DeclareStream(testStreamName, nil)
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("stream already exists"))
		err = testEnvironment.DeleteStream(testStreamName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Create two times Stream precondition fail", func() {
		err := testEnvironment.DeclareStream(testStreamName, nil)
		Expect(err).NotTo(HaveOccurred())
		err = testEnvironment.DeclareStream(testStreamName,
			&StreamOptions{
				MaxAge:         0,
				MaxLengthBytes: ByteCapacity{}.MB(100),
			})
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("precondition failed"))
		err = testEnvironment.DeleteStream(testStreamName)
		Expect(err).NotTo(HaveOccurred())
	})

	It("Create empty Stream  fail", func() {
		err := testEnvironment.DeclareStream("", nil)
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("stream Name can't be empty"))
	})

})
