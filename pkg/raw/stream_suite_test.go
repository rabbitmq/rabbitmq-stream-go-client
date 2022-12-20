package raw_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
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
	expectOffset1(rmq.connection.SetDeadline(time.Now().Add(rmq.deadlineDelta))).
		WithOffset(1).
		To(Succeed())

	serverConnReader := bufio.NewReader(rmq.connection)
	serverConnWriter := bufio.NewWriter(rmq.connection)

	// peer properties
	fourByteBuff := make([]byte, 4)
	n, err := io.ReadFull(serverConnReader, fourByteBuff)
	expectOffset1(err).ToNot(HaveOccurred())
	expectOffset1(n).To(BeNumerically("==", 4))

	var frameLen uint32
	frameLen = binary.BigEndian.Uint32(fourByteBuff)
	expectOffset1(frameLen).To(BeNumerically(">", 0))
	// TODO: perhaps decode bytes and assert on header + body?

	buff := make([]byte, frameLen)
	_, err = io.ReadFull(serverConnReader, buff)
	expectOffset1(err).ToNot(HaveOccurred())

	// peer properties response
	responseHeader := internal.NewHeader(
		38,
		internal.EncodeResponseCode(internal.CommandPeerProperties),
		1,
	)
	_, err = responseHeader.Write(serverConnWriter)
	expectOffset1(err).ToNot(HaveOccurred())

	var responseCode uint16
	responseCode = responseCodeFromContext(ctx, "peer-properties")
	propertiesResponse := internal.NewPeerPropertiesResponseWith(
		rmq.correlationIdSeq.next(),
		responseCode,
		map[string]string{"product": "mock-rabbitmq"},
	)
	binaryFrame, err := propertiesResponse.MarshalBinary()
	expectOffset1(err).ToNot(HaveOccurred())
	_, err = serverConnWriter.Write(binaryFrame)
	expectOffset1(err).ToNot(HaveOccurred())
	expectOffset1(serverConnWriter.Flush()).To(Succeed())
	if responseCode != 0x01 {
		return
	}

	// sasl handshake
	_, err = io.ReadFull(serverConnReader, fourByteBuff)
	expectOffset1(err).ToNot(HaveOccurred())
	frameLen = binary.BigEndian.Uint32(fourByteBuff)

	buff = make([]byte, frameLen)
	_, err = io.ReadFull(serverConnReader, buff)
	expectOffset1(err).ToNot(HaveOccurred())

	// sasl handshake response
	responseHeader = internal.NewHeader(
		29,
		internal.EncodeResponseCode(internal.CommandSaslHandshake),
		1,
	)
	_, err = responseHeader.Write(serverConnWriter)
	expectOffset1(err).ToNot(HaveOccurred())

	responseCode = responseCodeFromContext(ctx, "sasl-handshake")
	saslHandShakeResp := internal.NewSaslHandshakeResponseWith(
		rmq.correlationIdSeq.next(),
		responseCode,
		[]string{"FOOBAR", "PLAIN"},
	)
	binaryFrame, err = saslHandShakeResp.MarshalBinary()
	expectOffset1(err).ToNot(HaveOccurred())
	_, err = serverConnWriter.Write(binaryFrame)
	expectOffset1(err).ToNot(HaveOccurred())
	expectOffset1(serverConnWriter.Flush()).To(Succeed())
	if responseCode != 0x01 {
		return
	}

	// sasl authenticate
	_, err = io.ReadFull(serverConnReader, fourByteBuff)
	expectOffset1(err).ToNot(HaveOccurred())
	frameLen = binary.BigEndian.Uint32(fourByteBuff)

	buff = make([]byte, frameLen)
	_, err = io.ReadFull(serverConnReader, buff)
	expectOffset1(err).ToNot(HaveOccurred())

	responseHeader = internal.NewHeader(
		10,
		internal.EncodeResponseCode(internal.CommandSaslAuthenticate),
		1)
	_, err = responseHeader.Write(serverConnWriter)
	expectOffset1(err).ToNot(HaveOccurred())

	responseCode = responseCodeFromContext(ctx, "sasl-auth")
	saslAuthResp := internal.NewSaslAuthenticateResponseWith(
		rmq.correlationIdSeq.next(),
		responseCode,
		nil)
	binaryFrame, err = saslAuthResp.MarshalBinary()
	expectOffset1(err).ToNot(HaveOccurred())
	_, err = serverConnWriter.Write(binaryFrame)
	expectOffset1(err).ToNot(HaveOccurred())
	expectOffset1(serverConnWriter.Flush()).To(Succeed())
	if responseCode != 0x01 {
		return
	}

	// tune
	responseHeader = internal.NewHeader(
		12,
		internal.CommandTune,
		1)
	_, err = responseHeader.Write(serverConnWriter)
	expectOffset1(err).ToNot(HaveOccurred())

	tuneReq := internal.NewTuneResponse(8192, 60)
	_, err = tuneReq.Write(serverConnWriter)
	expectOffset1(err).ToNot(HaveOccurred())
	expectOffset1(serverConnWriter.Flush()).WithOffset(1).To(Succeed())

	// tune from client
	_, err = io.ReadFull(serverConnReader, fourByteBuff)
	expectOffset1(err).ToNot(HaveOccurred())
	frameLen = binary.BigEndian.Uint32(fourByteBuff)

	buff = make([]byte, frameLen)
	_, err = io.ReadFull(serverConnReader, buff)
	expectOffset1(err).ToNot(HaveOccurred())

	// open
	_, err = io.ReadFull(serverConnReader, fourByteBuff)
	expectOffset1(err).ToNot(HaveOccurred())
	frameLen = binary.BigEndian.Uint32(fourByteBuff)

	buff = make([]byte, frameLen)
	_, err = io.ReadFull(serverConnReader, buff)
	expectOffset1(err).ToNot(HaveOccurred())

	// open response
	responseHeader = internal.NewHeader(
		31,
		internal.EncodeResponseCode(internal.CommandOpen),
		1,
	)
	_, err = responseHeader.Write(serverConnWriter)
	expectOffset1(err).ToNot(HaveOccurred())

	responseCode = responseCodeFromContext(ctx, "open")
	openResp := internal.NewOpenResponseWith(
		rmq.correlationIdSeq.next(),
		responseCode,
		map[string]string{"welcome": "friend"},
	)
	binaryFrame, err = openResp.MarshalBinary()
	expectOffset1(err).ToNot(HaveOccurred())
	_, err = serverConnWriter.Write(binaryFrame)
	expectOffset1(err).ToNot(HaveOccurred())
	expectOffset1(serverConnWriter.Flush()).To(Succeed())
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQConnectionClose(ctx context.Context) {
	defer GinkgoRecover()
	expectOffset1(rmq.connection.SetDeadline(time.Now().Add(rmq.deadlineDelta))).
		To(Succeed())

	rw := bufio.NewReadWriter(bufio.NewReader(rmq.connection), bufio.NewWriter(rmq.connection))

	// close syncRequest
	headerRequest := new(internal.Header)
	expectOffset1(headerRequest.Read(rw.Reader)).To(Succeed())
	expectOffset1(headerRequest.Command()).To(BeNumerically("==", 0x0016))
	expectOffset1(headerRequest.Version()).To(BeNumerically("==", 1))

	buff := make([]byte, headerRequest.Length()-4)
	expectOffset1(io.ReadFull(rw.Reader, buff)).
		To(BeNumerically("==", headerRequest.Length()-4))

	bodyRequest := new(internal.CloseRequest)
	expectOffset1(bodyRequest.UnmarshalBinary(buff)).To(Succeed())
	expectOffset1(bodyRequest.ClosingCode()).To(BeNumerically("==", 1))
	expectOffset1(bodyRequest.ClosingReason()).To(Equal("kthxbye"))

	// close response
	headerResponse := internal.NewHeader(
		10,
		internal.EncodeResponseCode(internal.CommandClose),
		1,
	)
	expectOffset1(headerResponse.Write(rw.Writer)).To(BeNumerically("==", 8))

	response := internal.NewSimpleResponseWith(rmq.correlationIdSeq.next(), responseCodeFromContext(ctx))
	binaryResponse, err := response.MarshalBinary()
	expectOffset1(err).ToNot(HaveOccurred())
	_, err = rw.Write(binaryResponse)
	expectOffset1(err).ToNot(HaveOccurred())
	expectOffset1(rw.Flush()).To(Succeed())
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQDeclareStream(ctx context.Context, name string, args constants.StreamConfiguration) {
	defer GinkgoRecover()
	expectOffset1(rmq.connection.SetDeadline(time.Now().Add(time.Second))).
		To(Succeed())

	serverReader := bufio.NewReader(rmq.connection)

	header := new(internal.Header)
	expectOffset1(header.Read(serverReader)).To(Succeed())
	expectOffset1(header.Command()).To(BeNumerically("==", 0x000d))
	expectOffset1(header.Version()).To(BeNumerically("==", 1))

	buff := make([]byte, header.Length()-4)
	expectOffset1(io.ReadFull(serverReader, buff)).
		To(BeNumerically("==", header.Length()-4))

	body := new(internal.CreateRequest)
	expectOffset1(body.UnmarshalBinary(buff)).To(Succeed())
	expectOffset1(body.Stream()).To(Equal(name))
	expectOffset1(body.Arguments()).To(Equal(args))

	/// there server says ok! :)
	/// writing the response to the client
	writeResponse(ctx, rmq, bufio.NewWriter(rmq.connection), internal.CommandCreate)
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQDeleteStream(ctx context.Context, name string) {
	defer GinkgoRecover()
	expect := func(v any, extra ...any) Assertion {
		if extra != nil {
			return Expect(v, extra...).WithOffset(1)
		}
		return Expect(v).WithOffset(1)
	}
	expect(rmq.connection.SetDeadline(time.Now().Add(time.Second))).
		To(Succeed())

	serverReader := bufio.NewReader(rmq.connection)

	header := new(internal.Header)
	expect(header.Read(serverReader)).To(Succeed())
	expect(header.Command()).To(BeNumerically("==", 0x000e))
	expect(header.Version()).To(BeNumerically("==", 1))

	buff := make([]byte, header.Length()-4)
	expect(io.ReadFull(serverReader, buff)).
		To(BeNumerically("==", header.Length()-4))

	body := new(internal.DeleteRequest)
	expect(body.UnmarshalBinary(buff)).To(Succeed())
	expect(body.Stream()).To(Equal(name))

	/// there server says ok! :)
	/// writing the response to the client
	writeResponse(ctx, rmq, bufio.NewWriter(rmq.connection), internal.CommandDelete)
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQNewPublisher(ctx context.Context, publisherId uint8, publisherRef string, stream string) {
	defer GinkgoRecover()

	expectOffset1(rmq.connection.SetDeadline(time.Now().Add(time.Second))).
		To(Succeed())

	serverReader := bufio.NewReader(rmq.connection)

	header := new(internal.Header)
	expectOffset1(header.Read(serverReader)).To(Succeed())
	expectOffset1(header.Command()).To(BeNumerically("==", 0x0001))
	expectOffset1(header.Version()).To(BeNumerically("==", 1))

	buff := make([]byte, header.Length()-4)
	expectOffset1(io.ReadFull(serverReader, buff)).
		To(BeNumerically("==", header.Length()-4))

	body := new(internal.DeclarePublisherRequest)
	expectOffset1(body.UnmarshalBinary(buff)).To(Succeed())
	expectOffset1(body.PublisherId()).To(BeNumerically("==", publisherId))
	expectOffset1(body.Reference()).To(Equal(publisherRef))
	expectOffset1(body.Stream()).To(Equal(stream))

	/// there server says ok! :)
	/// writing the response to the client
	writeResponse(ctx, rmq, bufio.NewWriter(rmq.connection), internal.CommandDeclarePublisher)
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQDeletePublisher(ctx context.Context, publisherId uint8) {
	defer GinkgoRecover()

	expectOffset1(rmq.connection.SetDeadline(time.Now().Add(time.Second))).
		To(Succeed())

	serverReader := bufio.NewReader(rmq.connection)

	header := new(internal.Header)
	expectOffset1(header.Read(serverReader)).To(Succeed())
	expectOffset1(header.Command()).To(BeNumerically("==", 0x0006))
	expectOffset1(header.Version()).To(BeNumerically("==", 1))

	buff := make([]byte, header.Length()-4)
	expectOffset1(io.ReadFull(serverReader, buff)).
		To(BeNumerically("==", header.Length()-4))

	body := new(internal.DeletePublisherRequest)
	expectOffset1(body.UnmarshalBinary(buff)).To(Succeed())
	expectOffset1(body.PublisherId()).To(BeNumerically("==", publisherId))

	/// there server says ok! :)
	/// writing the response to the client
	writeResponse(ctx, rmq, bufio.NewWriter(rmq.connection), internal.CommandDeletePublisher)
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQServerClosesConnection() {
	defer GinkgoRecover()
	expectOffset1(rmq.connection.SetDeadline(time.Now().Add(rmq.deadlineDelta))).
		To(Succeed())

	rw := bufio.NewReadWriter(bufio.NewReader(rmq.connection), bufio.NewWriter(rmq.connection))

	// server closes the connection
	frameSize := 4 + // header
		4 + // correlation id
		2 + // closing code
		2 + len("admin says bye!") // closing-reason_size + closing-reason
	headerRequest := internal.NewHeader(frameSize, 0x0016, 1)
	expectOffset1(headerRequest.Write(rw.Writer)).To(BeNumerically("==", 8)) // frame_size + commandId + version

	bodyRequest := internal.NewCloseRequest(1, "admin says bye!")
	bodyRequest.SetCorrelationId(rmq.correlationIdSeq.next())
	expectOffset1(bodyRequest.Write(rw.Writer)).To(BeNumerically("==", frameSize-4)) // minus header size
	expectOffset1(rw.Flush()).To(Succeed())

	// client sends a response
	responseHeader := new(internal.Header)
	expectOffset1(responseHeader.Read(rw.Reader)).To(Succeed())

	responseBody := new(internal.SimpleResponse)
	expectOffset1(responseBody.Read(rw.Reader)).To(Succeed())
	expectOffset1(responseBody.ResponseCode()).To(BeNumerically("==", 1))
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQPublisherConfirms(pubId uint8, numOfConfirms int) {
	defer GinkgoRecover()
	expectOffset1(rmq.connection.SetDeadline(time.Now().Add(rmq.deadlineDelta))).
		To(Succeed())

	fakeConfirms := make([]uint64, numOfConfirms, numOfConfirms)
	for i := 0; i < len(fakeConfirms); i++ {
		fakeConfirms[i] = uint64(i)
	}

	// publish confirmation
	frameSize := 4 + // header
		1 + // publisher ID
		2 + // publishingIds count
		(8 * numOfConfirms) // N long uints
	header := internal.NewHeader(frameSize, 0x0003, 1)
	expectOffset1(header.Write(rmq.connection)).To(BeNumerically("==", 8),
		"expected to write 8 bytes")

	bodyRequest, err := internal.NewPublishConfirmResponse(pubId, fakeConfirms).MarshalBinary()
	expectOffset1(err).ToNot(HaveOccurred())
	_, err = rmq.connection.Write(bodyRequest)
	expectOffset1(err).ToNot(HaveOccurred())
}

func newContextWithResponseCode(ctx context.Context, respCode uint16, suffix ...string) context.Context {
	var key = "rabbitmq-stream.response-code"
	if suffix != nil {
		key += suffix[0]
	}
	return context.WithValue(ctx, key, respCode)
}

func responseCodeFromContext(ctx context.Context, suffix ...string) (responseCode uint16) {
	var key = "rabbitmq-stream.response-code"
	if suffix != nil {
		key += suffix[0]
	}
	v := ctx.Value(key)
	if v == nil {
		return constants.ResponseCodeOK
	}
	responseCode = v.(uint16)
	return
}

func writeResponse(ctx context.Context, rmq *fakeRabbitMQServer, serverWriter *bufio.Writer, commandId uint16) {
	responseHeader := internal.NewHeader(10, internal.EncodeResponseCode(commandId), 1)
	responseBody := internal.NewSimpleResponseWith(rmq.correlationIdSeq.next(), responseCodeFromContext(ctx))
	Expect(responseHeader.Write(serverWriter)).WithOffset(2).To(BeNumerically("==", 8))

	responseBodyBinary, err := responseBody.MarshalBinary()
	Expect(err).WithOffset(2).ToNot(HaveOccurred())
	Expect(serverWriter.Write(responseBodyBinary)).WithOffset(2).To(BeNumerically("==", 6))
	Expect(serverWriter.Flush()).WithOffset(2).To(Succeed())
}

func expectOffset1(v any, extra ...any) Assertion {
	return Expect(v, extra...).WithOffset(1)
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
