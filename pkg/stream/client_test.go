package stream

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Streaming testEnvironment", func() {
	var (
		testStreamName  string
		testEnvironment *Environment
	)

	BeforeEach(func() {
		client, err := NewEnvironment(nil)
		Expect(err).NotTo(HaveOccurred())
		testEnvironment = client
		testStreamName = uuid.New().String()
	})

	AfterEach(func() {
		Expect(testEnvironment.Close()).To(Succeed())
		Eventually(testEnvironment.IsClosed, time.Millisecond*300).Should(BeTrue(), "Expected testEnvironment to be closed")
	})

	It("Create Stream", func() {
		Expect(testEnvironment.DeclareStream(testStreamName, nil)).
			NotTo(HaveOccurred())
		Expect(testEnvironment.DeleteStream(testStreamName)).NotTo(HaveOccurred())
	})

	It("Create Stream with parameter SetMaxLengthBytes and SetMaxSegmentSizeBytes", func() {
		streamP := uuid.New().String()
		Expect(testEnvironment.DeclareStream(streamP,
			&StreamOptions{
				MaxLengthBytes: ByteCapacity{}.GB(2),
			},
		)).NotTo(HaveOccurred())
		Expect(testEnvironment.DeleteStream(streamP)).
			NotTo(HaveOccurred())

		Expect(testEnvironment.DeclareStream(streamP, NewStreamOptions().
			SetMaxSegmentSizeBytes(ByteCapacity{}.KB(500)))).NotTo(HaveOccurred())
		Expect(testEnvironment.DeleteStream(streamP)).
			NotTo(HaveOccurred())

	})

	It("Create Stream with parameter SetMaxLengthBytes Error", func() {
		By("Client Test")
		streamP := uuid.New().String()

		err := testEnvironment.DeclareStream(streamP, NewStreamOptions().
			SetMaxLengthBytes(ByteCapacity{}.From("not_a_valid_value")))

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
		err := testEnvironment.DeclareStream(streamP, NewStreamOptions().SetMaxAge(120*time.Hour))
		Expect(err).NotTo(HaveOccurred())
		err = testEnvironment.DeleteStream(streamP)
		Expect(err).NotTo(HaveOccurred())

	})

	It("Create two times Stream", func() {
		Expect(testEnvironment.DeclareStream(testStreamName, nil)).NotTo(HaveOccurred())
		err := testEnvironment.DeclareStream(testStreamName, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(testEnvironment.DeleteStream(testStreamName)).NotTo(HaveOccurred())
	})

	It("Stream Exists", func() {
		exists, err := testEnvironment.StreamExists(testStreamName)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(Equal(false))

		Expect(testEnvironment.DeclareStream(testStreamName, nil)).NotTo(HaveOccurred())
		exists, err = testEnvironment.StreamExists(testStreamName)
		Expect(err).NotTo(HaveOccurred())
		Expect(exists).To(Equal(true))
		Expect(testEnvironment.DeleteStream(testStreamName)).NotTo(HaveOccurred())
	})

	It("Create two times Stream precondition fail", func() {
		Expect(testEnvironment.DeclareStream(testStreamName, nil)).NotTo(HaveOccurred())
		err := testEnvironment.DeclareStream(testStreamName,
			&StreamOptions{
				MaxAge:         0,
				MaxLengthBytes: ByteCapacity{}.MB(100),
			})
		Expect(err).To(HaveOccurred())
		Expect(err).To(Equal(PreconditionFailed))
		Expect(testEnvironment.DeleteStream(testStreamName)).NotTo(HaveOccurred())
	})

	It("Create empty Stream  fail", func() {
		err := testEnvironment.DeclareStream("", nil)
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("stream Name can't be empty"))
	})
})
