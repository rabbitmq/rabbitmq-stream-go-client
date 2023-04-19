package raw_test

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"golang.org/x/exp/slog"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var logger *slog.Logger

func TestStream(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Stream Suite")
}

var _ = BeforeSuite(func() {
	h := slog.HandlerOptions{
		Level: slog.LevelDebug,
	}.NewTextHandler(GinkgoWriter)
	logger = slog.New(h)
})

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

func (rmq *fakeRabbitMQServer) fakeRabbitMQExchangeCommandVersions(ctx context.Context) {
	defer GinkgoRecover()

	expectOffset1(rmq.connection.SetDeadline(time.Now().Add(time.Second))).
		To(Succeed())

	serverRW := bufio.NewReadWriter(bufio.NewReader(rmq.connection), bufio.NewWriter(rmq.connection))

	header := &internal.Header{}
	expectOffset1(header.Read(serverRW.Reader)).To(Succeed())
	expectOffset1(header.Command()).To(BeNumerically("==", 0x001b))
	expectOffset1(header.Version()).To(BeNumerically("==", 1))

	buff := make([]byte, header.Length()-4)
	expectOffset1(io.ReadFull(serverRW, buff)).To(BeNumerically("==", header.Length()-4))

	body := &internal.ExchangeCommandVersionsRequest{}
	expectOffset1(body.UnmarshalBinary(buff)).To(Succeed())

	// there server says ok! :)
	// writing the response to the client
	frameSize := 4 + // header
		4 + // correlation ID
		2 + // response code
		4 + // slice len
		6 // one key + min-ver + max-ver
	header = internal.NewHeader(frameSize, 0x801b, 1)
	expectOffset1(header.Write(serverRW)).To(BeNumerically("==", 8))

	bodyResp := internal.NewExchangeCommandVersionsResponse(rmq.correlationIdSeq.next(), responseCodeFromContext(ctx, "exchange"), &internal.ChunkResponse{})
	b, err := bodyResp.MarshalBinary()
	expectOffset1(err).ToNot(HaveOccurred())
	expectOffset1(serverRW.Write(b)).To(BeNumerically("==", frameSize-4))
	expectOffset1(serverRW.Flush()).To(Succeed())
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQStoreOffset(ctx context.Context, reference, stream string, offset uint64) {
	defer GinkgoRecover()
	expectOffset1(rmq.connection.SetDeadline(time.Now().Add(time.Second))).To(Succeed())

	// Declare a server reader only, there is no response to StoreOffset
	serverReader := bufio.NewReader(bufio.NewReader(rmq.connection))

	header := &internal.Header{}
	expectOffset1(header.Read(serverReader)).To(Succeed())
	expectOffset1(header.Command()).To(BeNumerically("==", 0x000a))
	expectOffset1(header.Version()).To(BeNumerically("==", 1))

	buff := make([]byte, header.Length()-4)
	expectOffset1(io.ReadFull(serverReader, buff)).To(BeNumerically("==", header.Length()-4))

	body := &internal.StoreOffsetRequest{}
	expectOffset1(body.UnmarshalBinary(buff)).To(Succeed())
	Expect(body.Stream()).To(Equal(stream))
	Expect(body.Reference()).To(Equal(reference))
	Expect(body.Offset()).To(Equal(offset))
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQQueryOffset(ctx context.Context, offset uint64) {
	defer GinkgoRecover()

	expectOffset1(rmq.connection.SetDeadline(time.Now().Add(time.Second))).
		To(Succeed())

	serverRW := bufio.NewReadWriter(bufio.NewReader(rmq.connection), bufio.NewWriter(rmq.connection))

	header := &internal.Header{}
	expectOffset1(header.Read(serverRW.Reader)).To(Succeed())
	expectOffset1(header.Command()).To(BeNumerically("==", 0x000b))
	expectOffset1(header.Version()).To(BeNumerically("==", 1))

	buff := make([]byte, header.Length()-4)
	expectOffset1(io.ReadFull(serverRW, buff)).To(BeNumerically("==", header.Length()-4))

	body := &internal.QueryOffsetRequest{}
	expectOffset1(body.UnmarshalBinary(buff)).To(Succeed())
	Expect(body.Stream()).To(Equal("stream"))
	Expect(body.ConsumerReference()).To(Equal("reference"))

	// there server says ok! :)
	// writing the response to the client
	frameSize := 4 + // header
		4 + // correlation ID
		2 + // response code
		8 // offset
	header = internal.NewHeader(frameSize, 0x800b, 1)
	expectOffset1(header.Write(serverRW)).To(BeNumerically("==", 8))

	bodyResp := internal.NewQueryOffsetResponseWith(rmq.correlationIdSeq.next(), responseCodeFromContext(ctx, "query_offset"), offset)
	b, err := bodyResp.MarshalBinary()
	expectOffset1(err).ToNot(HaveOccurred())
	expectOffset1(serverRW.Write(b)).To(BeNumerically("==", frameSize-4))
	expectOffset1(serverRW.Flush()).To(Succeed())
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

func (rmq *fakeRabbitMQServer) fakeRabbitMQNewConsumer(
	ctx context.Context,
	subscriptionId uint8,
	stream string,
	offsetType uint16,
	offset uint64,
	credit uint16,
	properties constants.SubscribeProperties,
) {
	defer GinkgoRecover()

	expectOffset1(rmq.connection.SetDeadline(time.Now().Add(time.Second))).
		To(Succeed())

	serverReader := bufio.NewReader(rmq.connection)

	header := new(internal.Header)
	expectOffset1(header.Read(serverReader)).To(Succeed())
	expectOffset1(header.Command()).To(BeNumerically("==", 0x0007))
	expectOffset1(header.Version()).To(BeNumerically("==", 1))

	buff := make([]byte, header.Length()-4)
	expectOffset1(io.ReadFull(serverReader, buff)).
		To(BeNumerically("==", header.Length()-4))

	body := new(internal.SubscribeRequest)
	expectOffset1(body.UnmarshalBinary(buff)).To(Succeed())
	expectOffset1(body.SubscriptionId()).To(BeNumerically("==", subscriptionId))
	expectOffset1(body.Offset()).To(Equal(offset))
	expectOffset1(body.OffsetType()).To(Equal(offsetType))
	expectOffset1(body.Credit()).To(Equal(credit))
	expectOffset1(body.Stream()).To(Equal(stream))
	expectOffset1(body.Properties()).To(Equal(properties))

	/// there server says ok! :)
	/// writing the response to the client
	writeResponse(ctx, rmq, bufio.NewWriter(rmq.connection), internal.CommandSubscribe)

	// Send a delivery chunk
	serverWriter := bufio.NewWriter(rmq.connection)

	bodyResp := internal.ChunkResponse{
		SubscriptionId:   subscriptionId,
		CommittedChunkId: 123456789,
		MagicVersion:     42,
		ChunkType:        0,
		NumEntries:       1,
		NumRecords:       1,
		Timestamp:        11111111,
		Epoch:            11111111000,
		ChunkFirstOffset: 0,
		ChunkCrc:         12345,
		DataLength:       5,
		TrailerLength:    54321,
		Reserved:         0,
		Messages:         []byte("hello"),
	}
	frameSize := 4 + // header
		49 + 8 + // static chunk fields
		4 // len("hello")
	header = internal.NewHeader(frameSize, 0x0008, 2)

	_, err := header.Write(serverWriter)
	expectOffset1(err).ToNot(HaveOccurred())

	data, err := bodyResp.MarshalBinary()
	expectOffset1(err).ToNot(HaveOccurred())
	_, err = serverWriter.Write(data)
	expectOffset1(err).ToNot(HaveOccurred())
	expectOffset1(serverWriter.Flush()).To(Succeed())
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

	fakeConfirms := make([]uint64, numOfConfirms)
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

func (rmq *fakeRabbitMQServer) fakeRabbitMQMetadataQuery(ctx context.Context, stream string) {
	defer GinkgoRecover()
	expectOffset1(rmq.connection.SetDeadline(time.Now().Add(time.Second))).
		To(Succeed())
	serverReader := bufio.NewReader(rmq.connection)

	header := new(internal.Header)
	expectOffset1(header.Read(serverReader)).To(Succeed())
	expectOffset1(header.Command()).To(BeNumerically("==", 0x000f))

	buff := make([]byte, header.Length()-4)
	expectOffset1(io.ReadFull(serverReader, buff)).
		To(BeNumerically("==", header.Length()-4))

	body := new(internal.MetadataQuery)
	expectOffset1(body.UnmarshalBinary(buff)).To(Succeed())

	// metadataResponse
	frameSize := 4 + // header
		4 + // correlationID
		2 + // broker refernce
		9 + // broker host
		4 + // broker port
		8 + // streamMetadata streamName
		2 + // streamMetadata responseCode
		2 + // streamMetadata leaderReference
		8 // streamMetadata replicasreferences

	header = internal.NewHeader(frameSize, 0x800f, 1)
	expectOffset1(header.Write(rmq.connection)).To(BeNumerically("==", 8),
		"expected to write 8 bytes")

	responseCode := responseCodeFromContext(ctx, "metadata")
	var responseBody []byte
	var err error
	if stream == body.Stream() {
		responseBody, err = internal.NewMetadataResponse(rmq.correlationIdSeq.next(),
			5678,
			1,
			responseCode,
			2,
			"1.1.1.1",
			"stream",
			[]uint16{1, 2},
		).MarshalBinary()
	} else {
		responseBody, err = internal.NewMetadataResponse(rmq.correlationIdSeq.next(),
			0,
			0,
			responseCode,
			0,
			"",
			stream,
			[]uint16{},
		).MarshalBinary()
	}

	expectOffset1(err).ToNot(HaveOccurred())
	_, err = rmq.connection.Write(responseBody)
	expectOffset1(err).ToNot(HaveOccurred())
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQCredit(subscriptionId uint8, credits uint16) {
	defer GinkgoRecover()
	expectOffset1(rmq.connection.SetDeadline(time.Now().Add(time.Second))).
		To(Succeed())

	serverReader := bufio.NewReader(rmq.connection)

	header := new(internal.Header)
	expectOffset1(header.Read(serverReader)).To(Succeed())
	expectOffset1(header.Command()).To(BeNumerically("==", 0x0009))
	expectOffset1(header.Version()).To(BeNumerically("==", 1))

	buff := make([]byte, header.Length()-4)
	expectOffset1(io.ReadFull(serverReader, buff)).
		To(BeNumerically("==", header.Length()-4))

	body := new(internal.CreditRequest)
	expectOffset1(body.UnmarshalBinary(buff)).To(Succeed())
	expectOffset1(body.SubscriptionId()).To(Equal(subscriptionId))
	expectOffset1(body.Credit()).To(Equal(credits))
}

func (rmq *fakeRabbitMQServer) fakeRabbitMQCreditResponse(ctx context.Context, subscriptionId uint8) {
	defer GinkgoRecover()
	expectOffset1(rmq.connection.SetDeadline(time.Now().Add(time.Second))).
		To(Succeed())

	serverWriter := bufio.NewWriter(rmq.connection)

	header := internal.NewHeader(4, 0x8009, 1)
	expectOffset1(header.Write(serverWriter)).To(BeNumerically("==", 8))

	body := internal.NewCreditResponse(responseCodeFromContext(ctx, "credit"), subscriptionId)
	creditResponse, err := body.MarshalBinary()
	expectOffset1(err).NotTo(HaveOccurred())

	n, err := serverWriter.Write(creditResponse)
	expectOffset1(err).NotTo(HaveOccurred())
	expectOffset1(n).To(BeNumerically("==", 3))

	expectOffset1(serverWriter.Flush()).To(Succeed())
}

type ctxKey struct {
	string
}

func newContextWithResponseCode(ctx context.Context, respCode uint16, suffix ...string) context.Context {
	var key ctxKey
	if suffix != nil {
		key = ctxKey{"rabbitmq-stream.response-code" + suffix[0]}
	} else {
		key = ctxKey{"rabbitmq-stream.response-code"}
	}
	return context.WithValue(ctx, key, respCode)
}

func responseCodeFromContext(ctx context.Context, suffix ...string) (responseCode uint16) {
	var key ctxKey
	if suffix != nil {
		key = ctxKey{"rabbitmq-stream.response-code" + suffix[0]}
	} else {
		key = ctxKey{"rabbitmq-stream.response-code"}
	}
	v := ctx.Value(key)
	if v == nil {
		return streamResponseCodeOK
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
