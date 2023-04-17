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
		// TODO refactor NewStreamStat? reuse?
		It("returns the expected attributes", func() {
			streamStatsRequest := NewStreamStatsRequest("mystream")
			streamStatsRequest.SetCorrelationId(1234)

			Expect(streamStatsRequest.Key()).To(BeNumerically("==", 0x001c))
			Expect(streamStatsRequest.Version()).To(BeNumerically("==", 1))
			Expect(streamStatsRequest.CorrelationId()).To(BeNumerically("==", 1234))
			Expect(streamStatsRequest.stream).To(Equal("mystream"))
		})

		It("can calculate the size needed to binary encode itself", func() {
			streamStatsRequest := NewStreamStatsRequest("mystream")

			expectedSize := 2 + 2 + // keyId +Version
				4 + // uint32 for correlationID
				2 + 8 // uint16 for stream string + uint32 stream string length
			Expect(streamStatsRequest.SizeNeeded()).To(Equal(expectedSize))
		})

		It("encodes itself into a binary sequence", func() {
			streamStatsRequest := NewStreamStatsRequest("mystream")
			streamStatsRequest.SetCorrelationId(3)
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
})
