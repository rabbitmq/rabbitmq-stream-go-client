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
			createRequest := NewCreateRequest("mystream", map[string]string{"some-config": "it-works"})
			Expect(createRequest.Key()).To(BeNumerically("==", 0x000d))
			Expect(createRequest.Version()).To(BeNumerically("==", 1))
			Expect(createRequest.stream).To(Equal("mystream"))
			Expect(createRequest.arguments).To(HaveKeyWithValue("some-config", "it-works"))

			createRequest.SetCorrelationId(12345)
			Expect(createRequest.CorrelationId()).To(BeNumerically("==", 12345))

			expectedSize := streamProtocolKeySizeBytes + streamProtocolVersionSizeBytes +
				streamProtocolCorrelationIdSizeBytes +
				streamProtocolStringLenSizeBytes + len("mystream") +
				sizeNeededForMap(createRequest.arguments)

			Expect(createRequest.SizeNeeded()).To(BeNumerically("==", expectedSize))
		})

		It("encodes itself into a binary sequence", func() {
			c := NewCreateRequest("rabbit", map[string]string{"myarg": "myvalue"})
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
			expectedByteSequence = append(expectedByteSequence, []byte{0x00, 0x00, 0x00, 0x01}...)
			expectedByteSequence = append(expectedByteSequence, 0x00, 0x05)
			expectedByteSequence = append(expectedByteSequence, []byte("myarg")...)
			expectedByteSequence = append(expectedByteSequence, 0x00, 0x07)
			expectedByteSequence = append(expectedByteSequence, []byte("myvalue")...)

			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})

})
