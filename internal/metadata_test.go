package internal

import (
	"bufio"
	"bytes"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Internal/Metadata", func() {
	Context("Query", func() {
		It("knows the size needed to encode itself", func() {
			stream1 := "stream1"
			stream2 := "stream2"
			m := NewMetadataQuery([]string{stream1, stream2})
			m.SetCorrelationId(123)

			expectedSize := streamProtocolKeySizeBytes +
				streamProtocolVersionSizeBytes +
				streamProtocolCorrelationIdSizeBytes +
				streamProtocolSliceLenBytes +
				streamProtocolStringLenSizeBytes + len(stream1) +
				streamProtocolStringLenSizeBytes + len(stream2)

			Expect(m.SizeNeeded()).To(BeNumerically("==", expectedSize))
		})

		It("encodes itself into a binary sequence", func() {
			stream1 := "stream1"
			stream2 := "stream2"
			m := NewMetadataQuery([]string{stream1, stream2})
			m.SetCorrelationId(123)

			buff := &bytes.Buffer{}
			wr := bufio.NewWriter(buff)
			Expect(m.Write(wr)).To(BeNumerically("==", m.SizeNeeded()-streamProtocolCorrelationIdSizeBytes))
			Expect(wr.Flush()).To(Succeed())

			expectedByteSequence := []byte{
				0x00, 0x00, 0x00, 0x7B, // correlationId
				0x00, 0x00, 0x00, 0x02, // slice len bytes
				0x00, 0x07, // string length
				byte('s'), byte('t'), byte('r'), byte('e'), byte('a'), byte('m'), byte('1'),
				0x00, 0x07, // string length
				byte('s'), byte('t'), byte('r'), byte('e'), byte('a'), byte('m'), byte('2'),
			}

			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})

	Context("Response", func() {
		It("successfully decode a binary sequence into itself", func() {
			byteSequence := []byte{
				0x00, 0x00, 0x00, 0xff, // correlation id
				0x00, 0x01, // broker reference
				0x00, 0x07, // broker host length
				byte('1'), byte('.'), byte('1'), byte('.'), byte('1'), byte('.'), byte('1'), // broker host
				0x00, 0x00, 0x00, 0x02, // broker port
				0x00, 0x04, // stream name length
				byte('t'), byte('e'), byte('s'), byte('t'), // stream name
				0x00, 0x03, // response code
				0x00, 0x04, // leaderReference
				0x00, 0x00, 0x00, 0x03, // replica reference length
				0x00, 0x0a, 0x00, 0x0b, 0x00, 0x0c, // replica references
			}

			resp := new(MetadataResponse)
			Expect(resp.Read(bufio.NewReader(bytes.NewReader(byteSequence)))).To(Succeed())

			Expect(resp.correlationId).To(BeNumerically("==", 255))
			Expect(resp.broker.reference).To(BeNumerically("==", 1))
			Expect(resp.broker.host).To(Equal("1.1.1.1"))
			Expect(resp.broker.port).To(BeNumerically("==", 2))
			Expect(resp.streamMetadata.streamName).To(Equal("test"))
			Expect(resp.streamMetadata.responseCode).To(BeNumerically("==", 3))
			Expect(resp.streamMetadata.leaderReference).To(BeNumerically("==", 4))
			Expect(resp.streamMetadata.replicasReferences).To(HaveLen(3))
			Expect(resp.streamMetadata.replicasReferences).To(ConsistOf(uint16(10), uint16(11), uint16(12)))
		})
	})
})
