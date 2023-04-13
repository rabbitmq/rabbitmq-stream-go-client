package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("QueryOffset", func() {
	Describe("QueryOffsetRequest", func() {

		It("returns the size required to encode the frame", func() {
			queryOffsetRequest := NewQueryOffsetRequest("my_c", "my_stream")

			expectedSize := 2 + 2 + 4 + // key ID + version + correlation ID
				2 + 4 + // uint16 for the consumerReference string  + uint32 consumerReference string length
				2 + 9 // uint16 for the stream string  + uint32 stream string length
			Expect(queryOffsetRequest.SizeNeeded()).To(Equal(expectedSize))
		})

		It("has the expected fields", func() {
			queryOffsetRequest := NewQueryOffsetRequest("my_c", "my_stream")
			queryOffsetRequest.SetCorrelationId(123)
			Expect(queryOffsetRequest.Key()).To(BeNumerically("==", 0x000b))
			Expect(queryOffsetRequest.Stream()).To(Equal("my_stream"))
			Expect(queryOffsetRequest.ConsumerReference()).To(Equal("my_c"))
			Expect(queryOffsetRequest.CorrelationId()).To(BeNumerically("==", 123))

		})

		It("binary encodes the struct into a binary sequence", func() {
			queryOffsetRequest := NewQueryOffsetRequest("my_c", "my_stream")
			queryOffsetRequest.SetCorrelationId(123)
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)

			bytesWritten, err := queryOffsetRequest.Write(wr)
			Expect(err).ToNot(HaveOccurred())
			Expect(wr.Flush()).To(Succeed())

			Expect(bytesWritten).To(BeNumerically("==", queryOffsetRequest.SizeNeeded()-streamProtocolHeaderSizeBytes))

			expectedByteSequence := []byte{0x00, 0x00, 0x00, 123, // correlation id
				0x00, 0x04, byte('m'), byte('y'), byte('_'), byte('c'), // consumerReference len + consumerReference string
				0x00, 0x09, byte('m'), byte('y'), byte('_'), byte('s'), byte('t'), byte('r'), byte('e'), byte('a'), byte('m'), // stream len + stream string
			}
			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})

	})

	Describe("QueryOffsetResponse", func() {
		It("decodes a binary sequence into a struct", func() {
			someResponse := NewQueryOffsetResponse()
			binaryQueryOffset := []byte{
				0x00, 0x00, 0x00, 0xFF, // correlation id
				0x00, 0x0a, // response code
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // offset
			}

			buff := bytes.NewBuffer(binaryQueryOffset)
			reader := bufio.NewReader(buff)
			Expect(someResponse.Read(reader)).To(Succeed())

			Expect(someResponse.CorrelationId()).To(BeNumerically("==", 0xFF))
			Expect(someResponse.ResponseCode()).To(BeNumerically("==", 0x0a))
			Expect(someResponse.Offset()).To(BeNumerically("==", 7))
		})
	})
})
