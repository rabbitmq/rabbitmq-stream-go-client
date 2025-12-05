package stream

import (
	"bufio"
	"bytes"
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

	It("Stream Status", func() {
		Expect(testEnvironment.DeclareStream(testStreamName, nil)).
			NotTo(HaveOccurred())
		stats, err := testEnvironment.StreamStats(testStreamName)
		Expect(err).NotTo(HaveOccurred())
		Expect(stats).NotTo(BeNil())

		DeferCleanup(func() {
			Expect(testEnvironment.DeleteStream(testStreamName)).NotTo(HaveOccurred())
		})

		_, err = stats.FirstOffset()
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("FirstOffset not found for"))

		_, err = stats.LastOffset()
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("LastOffset not found for"))

		_, err = stats.CommittedChunkId()
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("CommittedChunkId not found for"))

		producer, err := testEnvironment.NewProducer(testStreamName, nil)
		Expect(err).NotTo(HaveOccurred())
		err = producer.BatchSend(CreateArrayMessagesForTesting(1_000))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(time.Millisecond * 800)
		Expect(producer.Close()).NotTo(HaveOccurred())

		statsAfter, err := testEnvironment.StreamStats(testStreamName)
		Expect(err).NotTo(HaveOccurred())
		Expect(statsAfter).NotTo(BeNil())

		offset, err := statsAfter.FirstOffset()
		Expect(err).NotTo(HaveOccurred())
		Expect(offset == 0).To(BeTrue())

		offset, err = statsAfter.LastOffset()
		Expect(err).NotTo(HaveOccurred())
		Expect(offset > 0).To(BeTrue())

		offset, err = statsAfter.CommittedChunkId()
		Expect(err).NotTo(HaveOccurred())
		Expect(offset > 0).To(BeTrue())
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

	It("Client.queryOffset won't hang if timeout happens", func() {
		cli := newClient("connName", nil, nil, nil, defaultSocketCallTimeout)
		cli.socket.writer = bufio.NewWriter(bytes.NewBuffer([]byte{}))
		cli.socketCallTimeout = time.Millisecond

		res, err := cli.queryOffset("cons", "stream")
		Expect(err.Error()).To(ContainSubstring("timeout 1 ms - waiting Code, operation: CommandQueryOffset"))
		Expect(res).To(BeZero())
	})

	It("Client.queryPublisherSequence won't hang if timeout happens", func() {
		cli := newClient("connName", nil, nil, nil, defaultSocketCallTimeout)
		cli.socket.writer = bufio.NewWriter(bytes.NewBuffer([]byte{}))
		cli.socketCallTimeout = time.Millisecond

		res, err := cli.queryPublisherSequence("ref", "stream")
		Expect(err.Error()).To(ContainSubstring("timeout 1 ms - waiting Code, operation: CommandQueryPublisherSequence"))
		Expect(res).To(BeZero())
	})

	It("Client.StreamStats won't hang if timeout happens", func() {
		cli := newClient("connName", nil, nil, nil, defaultSocketCallTimeout)
		cli.socket.writer = bufio.NewWriter(bytes.NewBuffer([]byte{}))
		cli.socketCallTimeout = time.Millisecond

		res, err := cli.StreamStats("stream")
		Expect(err.Error()).To(ContainSubstring("timeout 1 ms - waiting Code, operation: CommandStreamStatus"))
		Expect(res).To(BeNil())
	})

	It("Client.handleGenericResponse handles timeout and missing response gracefully", func() {
		cli := newClient("connName", nil, nil, nil, defaultSocketCallTimeout)

		// Simulate timeout: create a response and remove it immediately
		res := cli.coordinator.NewResponse(commandDeclarePublisher, "Simulated Test")
		err := cli.coordinator.RemoveResponseById(res.correlationid)
		Expect(err).To(BeNil())

		// Simulate receiving a response for the removed correlation id
		readerProtocol := &ReaderProtocol{
			CorrelationId: uint32(res.correlationid),
			ResponseCode:  responseCodeStreamNotAvailable,
		}
		cli.handleGenericResponse(readerProtocol, bufio.NewReader(bytes.NewBuffer([]byte{})))

	})

})
