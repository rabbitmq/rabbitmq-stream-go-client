package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Create", func() {
	Context("Request", func() {
		It("returns the expected attributes", func() {
			declarePublisherRequest := NewDeclarePublisherRequest(12, "myPublisherRef", "mystream")
			Expect(declarePublisherRequest.Key()).To(BeNumerically("==", 0x0001))
			Expect(declarePublisherRequest.Version()).To(BeNumerically("==", 1))
			Expect(declarePublisherRequest.stream).To(Equal("mystream"))

			declarePublisherRequest.SetCorrelationId(12346)
			Expect(declarePublisherRequest.CorrelationId()).To(BeNumerically("==", 12346))

			expectedSize := streamProtocolHeader +
				streamProtocolKeySizeUint8 +
				streamProtocolStringLenSizeBytes + len("myPublisherRef") + // publisher reference
				streamProtocolStringLenSizeBytes + len("mystream")

			Expect(declarePublisherRequest.SizeNeeded()).To(BeNumerically("==", expectedSize))
		})

		It("encodes itself into a binary sequence", func() {
			c := NewDeclarePublisherRequest(12, "myPublisherRef", "mystream")
			c.SetCorrelationId(42)

			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			Expect(c.Write(wr)).To(BeNumerically("==", c.SizeNeeded()-streamProtocolHeaderSizeBytes))
			Expect(wr.Flush()).To(Succeed())

			expectedByteSequence := []byte{
				0x00, 0x00, 0x00, 0x2a, // correlation ID
			}
			expectedByteSequence = append(expectedByteSequence, []byte{0x0c}...)       // publisher ID
			expectedByteSequence = append(expectedByteSequence, []byte{0x00, 0x0e}...) // publisherReference length
			expectedByteSequence = append(expectedByteSequence, []byte("myPublisherRef")...)
			expectedByteSequence = append(expectedByteSequence, []byte{0x00, 0x08}...) // stream length
			expectedByteSequence = append(expectedByteSequence, []byte("mystream")...)

			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})
})
