package stream_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"github.com/golang/mock/gomock"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/stream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"io"
	"net"
	"time"
)

var _ = Describe("RawClient",  func() {
	var (
		fakeServerConn net.Conn
		fakeClientConn net.Conn
		mockCtrl       *gomock.Controller
		fakeConn       *MockConn
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		fakeConn = NewMockConn(mockCtrl)
		fakeServerConn, fakeClientConn = net.Pipe()
	})

	AfterEach(func() {
		Expect(fakeClientConn.Close()).To(Succeed())
		Expect(fakeServerConn.Close()).To(Succeed())
	})

	It("configures the client", func() {
		// the connection should not be used until Connect()
		fakeConn.
			EXPECT().
			Read(gomock.Any()).
			Times(0)

		By("creating a new raw client")
		conf, err := stream.NewRawClientConfiguration()
		Expect(err).ToNot(HaveOccurred())

		streamClient := stream.NewRawClient(fakeConn, conf)
		Expect(streamClient).NotTo(BeNil())

		By("not starting a connection yet")
		Consistently(streamClient.IsOpen).WithTimeout(time.Millisecond*200).Should(BeFalse(),
			"expected stream client to not be open")
	})

	It("establishes and closes a connection to RabbitMQ", func(ctx SpecContext) {
		// setup fake server responses
		go fakeRabbitMQConnectionOpen(fakeServerConn)
		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())

		itCtx, cancel := context.WithTimeout(ctx, time.Second*6)
		defer cancel()

		conf, err := stream.NewRawClientConfiguration()
		Expect(err).ToNot(HaveOccurred())

		streamClient := stream.NewRawClient(fakeClientConn, conf)
		Eventually(streamClient.Connect).
			WithContext(itCtx).
			WithTimeout(time.Second).
			Should(Succeed(), "expected connection to succeed")
		Consistently(streamClient.IsOpen).
			WithTimeout(time.Second*2).
			Should(BeTrue(), "expected connection to be open")

		go fakeRabbitMQConnectionClose(fakeServerConn)
		// We need to renew the deadline
		Expect(fakeClientConn.SetDeadline(time.Now().Add(time.Second))).To(Succeed())

		Eventually(streamClient.Close).
			WithContext(itCtx).
			WithTimeout(time.Second).
			Should(Succeed())
		Consistently(streamClient.IsOpen).
			WithTimeout(time.Second*2).
			Should(BeFalse(), "expected connection to be closed")
	})
})

func fakeRabbitMQConnectionOpen(fakeConn net.Conn) {
	defer GinkgoRecover()
	Expect(fakeConn.SetDeadline(time.Now().Add(time.Second))).
		WithOffset(1).
		To(Succeed())

	serverConnReader := bufio.NewReader(fakeConn)
	serverConnWriter := bufio.NewWriter(fakeConn)

	// peer properties
	buff := make([]byte, 231)
	_, err := io.ReadFull(serverConnReader, buff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	// TODO: perhaps decode bytes and assert on header + body?

	responseHeader := internal.NewHeader(
		38,
		internal.EncodeResponseCode(internal.CommandPeerProperties),
		1,
	)
	_, err = responseHeader.Write(serverConnWriter)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	propertiesResponse := internal.NewPeerPropertiesResponseWith(
		1,
		0x0001,
		map[string]string{"product": "mock-rabbitmq"},
	)
	binaryFrame, err := propertiesResponse.MarshalBinary()
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	_, err = serverConnWriter.Write(binaryFrame)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	Expect(serverConnWriter.Flush()).WithOffset(1).To(Succeed())

	// sasl handshake
	buff = make([]byte, 12)
	_, err = io.ReadFull(serverConnReader, buff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	responseHeader = internal.NewHeader(
		29,
		internal.EncodeResponseCode(internal.CommandSaslHandshake),
		1,
	)
	_, err = responseHeader.Write(serverConnWriter)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	saslHandShakeResp := internal.NewSaslHandshakeResponseWith(
		2,
		0x01,
		[]string{"FOOBAR", "PLAIN"},
	)
	binaryFrame, err = saslHandShakeResp.MarshalBinary()
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	_, err = serverConnWriter.Write(binaryFrame)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	Expect(serverConnWriter.Flush()).WithOffset(1).To(Succeed())

	// sasl authenticate
	var frameLen uint32
	err = binary.Read(serverConnReader, binary.BigEndian, &frameLen)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	buff = make([]byte, frameLen)
	_, err = io.ReadFull(serverConnReader, buff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	responseHeader = internal.NewHeader(
		10, // todo
		internal.EncodeResponseCode(internal.CommandSaslAuthenticate),
		1)
	_, err = responseHeader.Write(serverConnWriter)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	saslAuthResp := internal.NewSaslAuthenticateResponseWith(
		3,
		0x01,
		nil)
	binaryFrame, err = saslAuthResp.MarshalBinary()
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	_, err = serverConnWriter.Write(binaryFrame)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	Expect(serverConnWriter.Flush()).WithOffset(1).To(Succeed())

	// tune
	responseHeader = internal.NewHeader(
		12,
		internal.CommandTune,
		1)
	_, err = responseHeader.Write(serverConnWriter)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	tuneReq := internal.NewTuneResponse(8192, 60)
	_, err = tuneReq.Write(serverConnWriter)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	Expect(serverConnWriter.Flush()).WithOffset(1).To(Succeed())

	err = binary.Read(serverConnReader, binary.BigEndian, &frameLen)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	buff = make([]byte, frameLen)
	_, err = io.ReadFull(serverConnReader, buff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	// open
	err = binary.Read(serverConnReader, binary.BigEndian, &frameLen)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	buff = make([]byte, frameLen)
	_, err = io.ReadFull(serverConnReader, buff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	responseHeader = internal.NewHeader(
		31,
		internal.EncodeResponseCode(internal.CommandOpen),
		1,
	)
	_, err = responseHeader.Write(serverConnWriter)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	openResp := internal.NewOpenResponseWith(4, 0x01,
		map[string]string{"welcome": "friend"})
	binaryFrame, err = openResp.MarshalBinary()
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	_, err = serverConnWriter.Write(binaryFrame)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	Expect(serverConnWriter.Flush()).WithOffset(1).To(Succeed())
}

func fakeRabbitMQConnectionClose(fakeConn net.Conn) {
	defer GinkgoRecover()
	expect := func(v interface{}) Assertion {
		return Expect(v).WithOffset(1)
	}

	expect(fakeConn.SetDeadline(time.Now().Add(time.Second))).
		To(Succeed())

	rw := bufio.NewReadWriter(bufio.NewReader(fakeConn), bufio.NewWriter(fakeConn))

	expectedBytes := 12 + len("kthxbye")
	buff := make([]byte, expectedBytes)
	n, err := rw.Read(buff)
	expect(err).ToNot(HaveOccurred())
	expect(n).To(BeNumerically("==", expectedBytes))

	headerResponse := internal.NewHeader(
		10,
		internal.EncodeResponseCode(internal.CommandClose),
		1,
	)
	_, err = headerResponse.Write(rw.Writer)
	expect(err).ToNot(HaveOccurred())

	response := internal.NewCloseResponse(5, internal.ResponseCodeOK)
	binaryResponse, err := response.MarshalBinary()
	expect(err).ToNot(HaveOccurred())
	_, err = rw.Write(binaryResponse)
	expect(err).ToNot(HaveOccurred())
	expect(rw.Flush()).To(Succeed())
}
