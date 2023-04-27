package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Partitions", func() {
	//PartitionsQuery => Key Version CorrelationId SuperStream
	//Key => uint16 // 0x0019
	//Version => uint16
	//CorrelationId => uint32
	//SuperStream => string
	Describe("Query", func() {
		It("has the expected attributes", func() {
			pq := NewPartitionsQuery("superStream")
			pq.SetCorrelationId(3)

			Expect(pq.Key()).To(BeNumerically("==", 0x0019))
			Expect(pq.Version()).To(BeNumerically("==", 1))
			Expect(pq.CorrelationId()).To(BeNumerically("==", 3))
			Expect(pq.SuperStream()).To(Equal("superStream"))
		})

		It("can calculate the size needed to encode itself", func() {
			pq := NewPartitionsQuery("superStream")
			pq.SetCorrelationId(3)

			expectedSize := 2 + 2 + // keyId +Version
				4 + // uint32 for correlationID
				2 + 11 // uint16 for stream string + uint32 stream string length
			Expect(pq.SizeNeeded()).To(Equal(expectedSize))
		})

		It("can encode itself into a binary sequence", func() {
			pq := NewPartitionsQuery("superStream")
			pq.SetCorrelationId(3)

			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			bytesWritten, err := pq.Write(wr)

			Expect(err).NotTo(HaveOccurred())
			Expect(wr.Flush()).To(Succeed())
			Expect(bytesWritten).To(BeNumerically("==", pq.SizeNeeded()-streamProtocolHeaderSizeBytes))

			expectedByteSequence := []byte{
				0x00, 0x00, 0x00, 0x03, // correlationID
				0x00, 0x0B, byte('s'), byte('u'), byte('p'), byte('e'), byte('r'),
				byte('S'), byte('t'), byte('r'), byte('e'), byte('a'), byte('m'), // superStream
			}

			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})

	//PartitionsResponse => Key Version CorrelationId ResponseCode [Stream]
	//Key => uint16 // 0x8019
	//Version => uint16
	//CorrelationId => uint32
	//ResponseCode => uint16
	//Stream => string
	Describe("Response", func() {
		It("can decode a binary sequence into itself", func() {
			pr := NewPartitionsResponse()
			binaryPR := []byte{
				0x00, 0x00, 0x00, 0x03, // correlation id
				0x00, 0x01, // responseCode
				0x00, 0x00, 0x00, 0x02, // slice len
				0x00, 0x02, // string len
				byte('s'), byte('1'), // stream 1 string
				0x00, 0x02, // string len
				byte('s'), byte('2'), // stream 2 string
			}
			buff := bytes.NewBuffer(binaryPR)
			reader := bufio.NewReader(buff)
			Expect(pr.Read(reader)).To(Succeed())

			Expect(pr.CorrelationId()).To(BeNumerically("==", 3))
			Expect(pr.ResponseCode()).To(BeNumerically("==", 1))
			Expect(pr.Streams()).To(Equal([]string{"s1", "s2"}))
		})
	})
})
