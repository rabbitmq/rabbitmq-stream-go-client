package stream

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
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
		cli := newClient(connectionParameters{
			connectionName:    "connName",
			broker:            nil,
			tcpParameters:     nil,
			saslConfiguration: nil,
			rpcTimeout:        defaultSocketCallTimeout,
		})
		cli.socket.writer = bufio.NewWriter(bytes.NewBuffer([]byte{}))
		cli.socketCallTimeout = time.Millisecond

		res, err := cli.queryOffset("cons", "stream")
		Expect(err.Error()).To(ContainSubstring("timeout 1 ms - waiting Code, operation: CommandQueryOffset"))
		Expect(res).To(BeZero())
	})

	It("Client.queryPublisherSequence won't hang if timeout happens", func() {
		cli := newClient(connectionParameters{
			connectionName:    "connName",
			broker:            nil,
			tcpParameters:     nil,
			saslConfiguration: nil,
			rpcTimeout:        defaultSocketCallTimeout,
		})
		cli.socket.writer = bufio.NewWriter(bytes.NewBuffer([]byte{}))
		cli.socketCallTimeout = time.Millisecond

		res, err := cli.queryPublisherSequence("ref", "stream")
		Expect(err.Error()).To(ContainSubstring("timeout 1 ms - waiting Code, operation: CommandQueryPublisherSequence"))
		Expect(res).To(BeZero())
	})

	It("Client.StreamStats won't hang if timeout happens", func() {
		cli := newClient(connectionParameters{
			connectionName:    "connName",
			broker:            nil,
			tcpParameters:     nil,
			saslConfiguration: nil,
			rpcTimeout:        defaultSocketCallTimeout,
		})
		cli.socket.writer = bufio.NewWriter(bytes.NewBuffer([]byte{}))
		cli.socketCallTimeout = time.Millisecond

		res, err := cli.StreamStats("stream")
		Expect(err.Error()).To(ContainSubstring("timeout 1 ms - waiting Code, operation: CommandStreamStatus"))
		Expect(res).To(BeNil())
	})

	It("Client.handleGenericResponse handles timeout and missing response gracefully", func() {
		cli := newClient(connectionParameters{
			connectionName:    "connName",
			broker:            nil,
			tcpParameters:     nil,
			saslConfiguration: nil,
			rpcTimeout:        defaultSocketCallTimeout,
		})
		// Simulate timeout: create a response and remove it immediately
		res := cli.coordinator.NewResponse(commandDeclarePublisher, "Simulated Test")
		err := cli.coordinator.RemoveResponseById(res.correlationid)
		Expect(err).To(BeNil())

		// Simulate receiving a response for the removed correlation ID
		readerProtocol := &ReaderProtocol{
			CorrelationId: uint32(res.correlationid),
			ResponseCode:  responseCodeStreamNotAvailable,
		}
		cli.handleGenericResponse(readerProtocol, bufio.NewReader(bytes.NewBuffer([]byte{})))

	})

})

var _ = Describe("Tune negotiation", func() {
	It("negotiatedMaxValue takes the smaller bound, or the larger when either side is unlimited (0)", func() {
		Expect(negotiatedMaxValue(512*1024, 1048576)).To(Equal(512 * 1024))
		Expect(negotiatedMaxValue(1048576, 512*1024)).To(Equal(512 * 1024))
		Expect(negotiatedMaxValue(1048576, 1048576)).To(Equal(1048576))
		Expect(negotiatedMaxValue(0, 1048576)).To(Equal(1048576))
		Expect(negotiatedMaxValue(1048576, 0)).To(Equal(1048576))
		Expect(negotiatedMaxValue(0, 0)).To(Equal(0))
	})

	It("handleTune negotiates the minimum frame size and heartbeat instead of echoing the broker's values", func() {
		const requested = 512 * 1024

		cli := &Client{coordinator: NewCoordinator()}
		cli.tuneState.requestedMaxFrameSize = requested
		cli.tuneState.requestedHeartbeat = 30
		resp := cli.coordinator.NewResponseWithName("tune")

		var body bytes.Buffer
		writeUInt(&body, 1048576) // broker frame max advertised in the TUNE frame
		writeUInt(&body, 60)      // broker heartbeat
		cli.handleTune(bufio.NewReader(&body))

		tr, ok := (<-resp.data).(tuneResponse)
		Expect(ok).To(BeTrue())
		Expect(tr.maxFrameSize).To(Equal(requested))
		// Heartbeat is negotiated the same way: min(requested, broker).
		Expect(tr.heartbeat).To(Equal(30))
	})
})

var _ = Describe("Frame size enforcement", func() {
	// An over-sized command frame must fail fast, not be sent and block until timeout.
	It("fails fast on an over-sized command frame and keeps the connection usable", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().SetRequestedMaxFrameSize(100))
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(env.Close()).To(Succeed()) }()

		// CreateStream for this stream + option is ~117 bytes > 100.
		declareDone := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			declareDone <- env.DeclareStream(uuid.New().String(),
				&StreamOptions{MaxLengthBytes: ByteCapacity{}.GB(2)})
		}()
		var declErr error
		Eventually(declareDone, 3*time.Second).Should(Receive(&declErr),
			"DeclareStream must return fast, not hang on the 10s broker timeout")
		Expect(declErr).To(HaveOccurred())

		// The connection must still be usable after the rejection.
		healthy := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			_, e := env.StreamExists(uuid.New().String())
			healthy <- e
		}()
		var hErr error
		Eventually(healthy, 3*time.Second).Should(Receive(&hErr),
			"a valid operation after the rejection must not hang")
		Expect(hErr).NotTo(HaveOccurred())
	})

	// CreateStream frame = 55 + len(streamName); land exactly at and one byte over the limit.
	It("accepts a command frame at exactly the negotiated max and rejects one byte over", func() {
		const fm = 100
		env, err := NewEnvironment(NewEnvironmentOptions().SetRequestedMaxFrameSize(fm))
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(env.Close()).To(Succeed()) }()

		atLimit := strings.Repeat("a", fm-55)   // frame == 100
		overLimit := strings.Repeat("b", fm-54) // frame == 101

		Expect(env.DeclareStream(atLimit, nil)).NotTo(HaveOccurred(), "a frame of exactly the negotiated max must be accepted")
		Expect(env.DeleteStream(atLimit)).NotTo(HaveOccurred())

		err = env.DeclareStream(overLimit, nil)
		Expect(err).To(MatchError(FrameTooLarge), "a frame one byte over the max must be rejected by the client")
	})

	It("re-negotiates the frame max and re-arms the guard after a locator reconnect", func() {
		env, err := NewEnvironment(NewEnvironmentOptions().SetRequestedMaxFrameSize(100))
		Expect(err).NotTo(HaveOccurred())
		defer func() { Expect(env.Close()).To(Succeed()) }()

		// Force the locator's connection closed so the next operation reconnects.
		env.locator.client.socket.shutdown(fmt.Errorf("forced close for reconnect test"))
		Expect(env.locator.client.socket.isOpen()).To(BeFalse())

		_, err = env.StreamExists(uuid.New().String())
		Expect(err).NotTo(HaveOccurred())
		Expect(env.locator.client.socket.isOpen()).To(BeTrue())
		Expect(env.locator.client.maxFrameSize()).To(Equal(100), "frame max must be re-negotiated on the reconnected client")

		declErr := make(chan error, 1)
		go func() {
			defer GinkgoRecover()
			declErr <- env.DeclareStream(uuid.New().String(), &StreamOptions{MaxLengthBytes: ByteCapacity{}.GB(2)})
		}()
		var e error
		Eventually(declErr, 3*time.Second).Should(Receive(&e), "DeclareStream must not hang after reconnect")
		Expect(e).To(MatchError(FrameTooLarge))
	})
})
