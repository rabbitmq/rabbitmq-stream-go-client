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
			deleteRequest := NewDeletePublisherRequest(27)
			Expect(deleteRequest.Key()).To(BeNumerically("==", 0x0006))
			Expect(deleteRequest.Version()).To(BeNumerically("==", 1))
			Expect(deleteRequest.PublisherId()).To(BeNumerically("==", 27))

			deleteRequest.SetCorrelationId(12349)
			Expect(deleteRequest.CorrelationId()).To(BeNumerically("==", 12349))

			expectedSize := streamProtocolHeader + streamProtocolKeySizeUint8

			Expect(deleteRequest.SizeNeeded()).To(BeNumerically("==", expectedSize))
		})

		It("encodes itself into a binary sequence", func() {
			c := NewDeletePublisherRequest(155)
			c.SetCorrelationId(42)

			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			Expect(c.Write(wr)).To(BeNumerically("==", c.SizeNeeded()-streamProtocolHeaderSizeBytes))
			Expect(wr.Flush()).To(Succeed())

			expectedByteSequence := []byte{
				0x00, 0x00, 0x00, 0x2a, // correlation ID
				0x9B, // 155
			}

			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})

})
