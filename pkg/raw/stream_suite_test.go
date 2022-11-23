package raw_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	"io"
	"net"
	"os"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestStream(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Stream Suite")
}

type autoIncrementingSequence struct {
	last uint32
}

func (s *autoIncrementingSequence) next() uint32 {
	s.last += 1
	return s.last
}

type fakeRabbitMQServer struct {
	correlationIdSeq autoIncrementingSequence
	connection       net.Conn
	deadlineDelta    time.Duration
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQConnectionOpen(ctx context.Context) {
	defer GinkgoRecover()

	Expect(rmq.connection.SetDeadline(time.Now().Add(rmq.deadlineDelta))).
		WithOffset(1).
		To(Succeed())

	serverConnReader := bufio.NewReader(rmq.connection)
	serverConnWriter := bufio.NewWriter(rmq.connection)

	// peer properties
	fourByteBuff := make([]byte, 4)
	n, err := io.ReadFull(serverConnReader, fourByteBuff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	Expect(n).WithOffset(1).To(BeNumerically("==", 4))

	var frameLen uint32
	frameLen = binary.BigEndian.Uint32(fourByteBuff)
	Expect(frameLen).WithOffset(1).To(BeNumerically(">", 0))
	// TODO: perhaps decode bytes and assert on header + body?

	buff := make([]byte, frameLen)
	_, err = io.ReadFull(serverConnReader, buff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	// peer properties response
	responseHeader := internal.NewHeader(
		38,
		internal.EncodeResponseCode(internal.CommandPeerProperties),
		1,
	)
	_, err = responseHeader.Write(serverConnWriter)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	propertiesResponse := internal.NewPeerPropertiesResponseWith(
		rmq.correlationIdSeq.next(),
		0x0001,
		map[string]string{"product": "mock-rabbitmq"},
	)
	binaryFrame, err := propertiesResponse.MarshalBinary()
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	_, err = serverConnWriter.Write(binaryFrame)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	Expect(serverConnWriter.Flush()).WithOffset(1).To(Succeed())

	// sasl handshake
	_, err = io.ReadFull(serverConnReader, fourByteBuff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	frameLen = binary.BigEndian.Uint32(fourByteBuff)

	buff = make([]byte, frameLen)
	_, err = io.ReadFull(serverConnReader, buff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	// sasl handshake response
	responseHeader = internal.NewHeader(
		29,
		internal.EncodeResponseCode(internal.CommandSaslHandshake),
		1,
	)
	_, err = responseHeader.Write(serverConnWriter)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	saslHandShakeResp := internal.NewSaslHandshakeResponseWith(
		rmq.correlationIdSeq.next(),
		0x01,
		[]string{"FOOBAR", "PLAIN"},
	)
	binaryFrame, err = saslHandShakeResp.MarshalBinary()
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	_, err = serverConnWriter.Write(binaryFrame)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	Expect(serverConnWriter.Flush()).WithOffset(1).To(Succeed())

	// sasl authenticate
	_, err = io.ReadFull(serverConnReader, fourByteBuff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	frameLen = binary.BigEndian.Uint32(fourByteBuff)

	buff = make([]byte, frameLen)
	_, err = io.ReadFull(serverConnReader, buff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	responseHeader = internal.NewHeader(
		10,
		internal.EncodeResponseCode(internal.CommandSaslAuthenticate),
		1)
	_, err = responseHeader.Write(serverConnWriter)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	saslAuthResp := internal.NewSaslAuthenticateResponseWith(
		rmq.correlationIdSeq.next(),
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

	// tune from client
	_, err = io.ReadFull(serverConnReader, fourByteBuff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	frameLen = binary.BigEndian.Uint32(fourByteBuff)

	buff = make([]byte, frameLen)
	_, err = io.ReadFull(serverConnReader, buff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	// open
	_, err = io.ReadFull(serverConnReader, fourByteBuff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	frameLen = binary.BigEndian.Uint32(fourByteBuff)

	buff = make([]byte, frameLen)
	_, err = io.ReadFull(serverConnReader, buff)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	// open response
	responseHeader = internal.NewHeader(
		31,
		internal.EncodeResponseCode(internal.CommandOpen),
		1,
	)
	_, err = responseHeader.Write(serverConnWriter)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())

	openResp := internal.NewOpenResponseWith(
		rmq.correlationIdSeq.next(),
		0x01,
		map[string]string{"welcome": "friend"},
	)
	binaryFrame, err = openResp.MarshalBinary()
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	_, err = serverConnWriter.Write(binaryFrame)
	Expect(err).WithOffset(1).ToNot(HaveOccurred())
	Expect(serverConnWriter.Flush()).WithOffset(1).To(Succeed())
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQConnectionClose(ctx context.Context) {
	defer GinkgoRecover()
	expect := func(v interface{}) Assertion {
		return Expect(v).WithOffset(1)
	}
	expect(rmq.connection.SetDeadline(time.Now().Add(rmq.deadlineDelta))).
		To(Succeed())

	rw := bufio.NewReadWriter(bufio.NewReader(rmq.connection), bufio.NewWriter(rmq.connection))

	// close request
	buff := make([]byte, 4)
	n, err := io.ReadFull(rw.Reader, buff)
	expect(err).ToNot(HaveOccurred())
	expect(n).To(BeNumerically("==", 4))

	var frameLen uint32
	frameLen = binary.BigEndian.Uint32(buff)
	buff = make([]byte, frameLen)
	n, err = io.ReadFull(rw.Reader, buff)
	expect(err).ToNot(HaveOccurred())

	expectedBytes := 12 + len("kthxbye")
	expect(n).To(BeNumerically("==", expectedBytes))

	// close response
	headerResponse := internal.NewHeader(
		10,
		internal.EncodeResponseCode(internal.CommandClose),
		1,
	)
	_, err = headerResponse.Write(rw.Writer)
	expect(err).ToNot(HaveOccurred())

	response := internal.NewCloseResponse(rmq.correlationIdSeq.next(), responseCodeFromContext(ctx))
	binaryResponse, err := response.MarshalBinary()
	expect(err).ToNot(HaveOccurred())
	_, err = rw.Write(binaryResponse)
	expect(err).ToNot(HaveOccurred())
	expect(rw.Flush()).To(Succeed())
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQDeclareStream(ctx context.Context) {
	defer GinkgoRecover()
	expect := func(v interface{}) Assertion {
		return Expect(v).WithOffset(1)
	}
	expect(rmq.connection.SetDeadline(time.Now().Add(time.Second))).
		To(Succeed())

	serverReader := bufio.NewReader(rmq.connection)

	buffLen := 14 + len("test-stream") + 4 + 2 + len("some-key") + 2 + len("some-value")
	body := new(internal.CreateRequest)
	buff := make([]byte, buffLen)
	full, err := io.ReadFull(serverReader, buff)
	expect(err).ToNot(HaveOccurred())
	expect(full).To(BeNumerically("==", buffLen))

	header := new(internal.Header)
	expect(header.Read(bufio.NewReader(bytes.NewReader(buff)))).To(Succeed())
	expect(header.Command()).To(BeNumerically("==", 0x000d))
	expect(header.Version()).To(BeNumerically("==", 1))

	expect(body.UnmarshalBinary(buff[8:])).To(Succeed())
	expect(body.Stream()).To(Equal("test-stream"))
	expect(body.Arguments()).To(HaveKeyWithValue("some-key", "some-value"))

	/// there server says ok! :)
	/// writing the response to the client
	serverWriter := bufio.NewWriter(rmq.connection)

	responseHeader := internal.NewHeader(10, internal.EncodeResponseCode(internal.CommandCreate), 1)
	responseCode := responseCodeFromContext(ctx)
	responseBody := internal.NewCreateResponseWith(rmq.correlationIdSeq.next(), responseCode)
	Expect(responseHeader.Write(serverWriter)).WithOffset(1).To(BeNumerically("==", 8))
	responseBodyBinary, err := responseBody.MarshalBinary()
	expect(err).ToNot(HaveOccurred())
	Expect(serverWriter.Write(responseBodyBinary)).WithOffset(1).To(BeNumerically("==", 6))
	expect(serverWriter.Flush()).To(Succeed())
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQDeleteStream(ctx context.Context) {
	defer GinkgoRecover()
	expect := func(v interface{}) Assertion {
		return Expect(v).WithOffset(1)
	}
	expect(rmq.connection.SetDeadline(time.Now().Add(time.Second))).
		To(Succeed())

	serverReader := bufio.NewReader(rmq.connection)

	buffLen := 14 + len("test-stream")
	body := new(internal.DeleteRequest)
	buff := make([]byte, buffLen)
	full, err := io.ReadFull(serverReader, buff)
	expect(err).ToNot(HaveOccurred())
	expect(full).To(BeNumerically("==", buffLen))

	header := new(internal.Header)
	expect(header.Read(bufio.NewReader(bytes.NewReader(buff)))).To(Succeed())
	expect(header.Command()).To(BeNumerically("==", 0x000e))
	expect(header.Version()).To(BeNumerically("==", 1))

	expect(body.UnmarshalBinary(buff[8:])).To(Succeed())
	expect(body.Stream()).To(Equal("test-stream"))

	/// there server says ok! :)
	/// writing the response to the client
	serverWriter := bufio.NewWriter(rmq.connection)

	responseHeader := internal.NewHeader(10, internal.EncodeResponseCode(internal.CommandDelete), 1)
	responseCode := responseCodeFromContext(ctx)
	responseBody := internal.NewCreateResponseWith(rmq.correlationIdSeq.next(), responseCode)
	Expect(responseHeader.Write(serverWriter)).WithOffset(1).To(BeNumerically("==", 8))
	responseBodyBinary, err := responseBody.MarshalBinary()
	expect(err).ToNot(HaveOccurred())
	Expect(serverWriter.Write(responseBodyBinary)).WithOffset(1).To(BeNumerically("==", 6))
	expect(serverWriter.Flush()).To(Succeed())
}

func newContextWithResponseCode(ctx context.Context, respCode uint16) context.Context {
	return context.WithValue(ctx, "rabbitmq-stream.response-code", respCode)
}

func responseCodeFromContext(ctx context.Context) (responseCode uint16) {
	v := ctx.Value("rabbitmq-stream.response-code")
	if v == nil {
		return internal.ResponseCodeOK
	}
	responseCode = v.(uint16)
	return
}

func bufferDrainer(ctx context.Context, fakeServerConn net.Conn) {
	// Go routine to drain the 'server' pipe
	// Required for 'client' pipe Flush() to return
	defer GinkgoRecover()

	buffer := make([]byte, 1024)
	for {
		Expect(fakeServerConn.SetDeadline(time.Now().Add(time.Millisecond * 100))).WithOffset(1).To(Succeed())
		select {
		case <-ctx.Done():
			return
		default:
			_, err := fakeServerConn.Read(buffer)
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			}
			if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, io.EOF) {
				// ErrClosedPipe is ok because the test can pass and leave this routine in the background.
				// When that happens, Ginkgo.DeferCleanup closes the net.Pipe while this routine
				// is trying to Read(), failing the suite since it expects no errors in DeferCleanup.
				// When ErrClosedPipe happens, the context is already cancelled and this loop will end.
				// io.EOF is ok because it is graceful end of byte stream
				return
			}
			Expect(err).WithOffset(1).ToNot(HaveOccurred(), "unexpected error reading server fake conn")
		}
	}
}
