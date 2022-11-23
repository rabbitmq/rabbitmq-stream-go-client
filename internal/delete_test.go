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
			deleteRequest := NewDeleteRequest("mystream")
			Expect(deleteRequest.Key()).To(BeNumerically("==", 0x000e))
			Expect(deleteRequest.Version()).To(BeNumerically("==", 1))
			Expect(deleteRequest.stream).To(Equal("mystream"))

			deleteRequest.SetCorrelationId(12345)
			Expect(deleteRequest.CorrelationId()).To(BeNumerically("==", 12345))

			expectedSize := streamProtocolKeySizeBytes + streamProtocolVersionSizeBytes +
				streamProtocolCorrelationIdSizeBytes +
				streamProtocolStringLenSizeBytes + len("mystream")

			Expect(deleteRequest.SizeNeeded()).To(BeNumerically("==", expectedSize))
		})

		It("encodes itself into a binary sequence", func() {
			c := NewDeleteRequest("rabbit")
			c.SetCorrelationId(42)

			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			Expect(c.Write(wr)).To(BeNumerically("==", c.SizeNeeded()-streamProtocolHeaderSizeBytes))
			Expect(wr.Flush()).To(Succeed())

			expectedByteSequence := []byte{
				0x00, 0x00, 0x00, 0x2a, // correlation ID
				0x00, 0x06, // string size
			}
			expectedByteSequence = append(expectedByteSequence, []byte("rabbit")...)

			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})

	Context("Response", func() {
		It("decodes a binary sequence", func() {
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			_, err := writeMany(wr, uint32(12346), uint16(104))
			Expect(err).ToNot(HaveOccurred())
			Expect(wr.Flush()).To(Succeed())

			response := SimpleResponse{}
			Expect(response.Read(bufio.NewReader(buff))).To(Succeed())

			Expect(response.correlationId).To(BeNumerically("==", 12346))
			Expect(response.responseCode).To(BeNumerically("==", 104))
		})
	})
})
