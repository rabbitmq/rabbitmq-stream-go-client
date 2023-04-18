package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("StreamStats", func() {

	//StreamStatsRequest => Key Version CorrelationId Stream
	//Key => uint16 // 0x001c
	//Version => uint16
	//CorrelationId => uint32
	//Stream => string
	Context("Request", func() {
		var streamStatsRequest *StreamStatsRequest

		BeforeEach(func() {
			streamStatsRequest = NewStreamStatsRequest("mystream")
			streamStatsRequest.SetCorrelationId(3)
		})

		It("returns the expected attributes", func() {
			Expect(streamStatsRequest.Key()).To(BeNumerically("==", 0x001c))
			Expect(streamStatsRequest.Version()).To(BeNumerically("==", 1))
			Expect(streamStatsRequest.CorrelationId()).To(BeNumerically("==", 3))
			Expect(streamStatsRequest.Stream()).To(Equal("mystream"))
		})

		It("can calculate the size needed to binary encode itself", func() {
			expectedSize := 2 + 2 + // keyId +Version
				4 + // uint32 for correlationID
				2 + 8 // uint16 for stream string + uint32 stream string length
			Expect(streamStatsRequest.SizeNeeded()).To(Equal(expectedSize))
		})

		It("encodes itself into a binary sequence", func() {
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			bytesWritten, err := streamStatsRequest.Write(wr)

			Expect(err).NotTo(HaveOccurred())
			Expect(wr.Flush()).To(Succeed())
			Expect(bytesWritten).To(BeNumerically("==", streamStatsRequest.SizeNeeded()-streamProtocolHeaderSizeBytes))

			expectedByteSequence := []byte{
				0x00, 0x00, 0x00, 0x03, // correlationID
				0x00, 0x08, byte('m'), byte('y'), byte('s'), byte('t'), byte('r'), byte('e'), byte('a'), byte('m'), // stream
			}

			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})

	// StreamStatsResponse => Key Version CorrelationId ResponseCode Stats
	//  Key => uint16 // 0x801c
	//  Version => uint16
	//  CorrelationId => uint32
	//  ResponseCode => uint16
	//  Stats => [Statistic]
	//  Statistic => Key Value
	//  Key => string
	//  Value => int64
	Context("Response", func() {
		It("decodes a binary sequence into itself", func() {
			StreamStatsResponse := NewStreamStatsResponse()
			binaryStreamStats := []byte{
				0x00, 0x00, 0x00, 0x03, // correlation id
				0x00, 0x01, // responseCode
				0x00, 0x00, 0x00, 0x02, // stats map size
				0x00, 0x03, // key string length
				byte('c'), byte('p'), byte('u'), // key string
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, // int 64 value
				0x00, 0x03, // key string length
				byte('m'), byte('e'), byte('m'), // key string
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x19, // int 64 value
			}

			buff := bytes.NewBuffer(binaryStreamStats)
			reader := bufio.NewReader(buff)
			Expect(StreamStatsResponse.Read(reader)).To(Succeed())

			Expect(StreamStatsResponse.CorrelationId()).To(BeNumerically("==", 3))
			Expect(StreamStatsResponse.ResponseCode()).To(BeNumerically("==", 1))
			Expect(StreamStatsResponse.Stats).To(Equal(map[string]int64{"cpu": int64(50), "mem": int64(25)}))
		})
	})
})
