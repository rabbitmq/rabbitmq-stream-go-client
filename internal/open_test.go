package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
)

var _ = Describe("Open", func() {
	Context("Request", func() {
		It("has the correct attributes", func() {
			openReq := NewOpenRequest("some-vhost")
			openReq.SetCorrelationId(1)

			Expect(openReq.virtualHost).To(Equal("some-vhost"))
			Expect(openReq.CorrelationId()).To(BeNumerically("==", 1))
			Expect(openReq.Version()).To(BeNumerically("==", 1))
			Expect(openReq.Key()).To(BeNumerically("==", 0x0015))
			Expect(openReq.SizeNeeded()).
				To(BeNumerically(
					"==",
					streamProtocolKeySizeBytes+
						streamProtocolVersionSizeBytes+
						streamProtocolCorrelationIdSizeBytes+
						streamProtocolStringLenSizeBytes+
						len("some-vhost")))
		})

		It("binary encodes", func() {
			openReq := NewOpenRequest("rabbit-cool")
			openReq.SetCorrelationId(15)

			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			_, err := openReq.Write(wr)
			Expect(err).ToNot(HaveOccurred())
			Expect(wr.Flush()).To(Succeed())

			expectedByteSequence := []byte{
				0x00, 0x00, 0x00, 0x0f, // correlation id
				0x00, 0x0b, // string len
			}
			expectedByteSequence = append(expectedByteSequence, []byte("rabbit-cool")...)

			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})

	Context("Response", func() {
		It("decodes from a binary sequence", func() {
			binaryFrame := []byte{
				0x00, 0x00, 0x00, 0xff, // correlation id
				0x00, 0x7f, // response code
				0x00, 0x00, 0x00, 0x02, // map len
				0x00, 0x01, // key len
				byte('a'),
				0x00, 0x01, // value len
				byte('b'),
				0x00, 0x03, // key len
			}
			binaryFrame = append(binaryFrame, []byte("foo")...)
			binaryFrame = append(binaryFrame, 0x00, 0x03) // value len
			binaryFrame = append(binaryFrame, []byte("bar")...)

			openResp := new(OpenResponse)
			Expect(openResp.Read(bufio.NewReader(bytes.NewReader(binaryFrame)))).To(Succeed())

			Expect(openResp.CorrelationId()).To(BeNumerically("==", 255))
			Expect(openResp.responseCode).To(BeNumerically("==", 127))
			Expect(openResp.connectionProperties).To(HaveLen(2))
			Expect(openResp.connectionProperties).To(
				MatchAllKeys(Keys{
					"a":   Equal("b"),
					"foo": Equal("bar"),
				},
				))
		})
	})
})
