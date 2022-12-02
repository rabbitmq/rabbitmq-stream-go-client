package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Close", func() {
	Context("Request", func() {
		It("has the expected attributes", func() {
			closeReq := NewCloseRequest(1, "time to go home")
			closeReq.SetCorrelationId(2)

			Expect(closeReq.CorrelationId()).To(BeNumerically("==", 2))
			Expect(closeReq.Version()).To(BeNumerically("==", 1))
			Expect(closeReq.Key()).To(BeNumerically("==", 22))
			Expect(closeReq.ClosingCode()).To(BeNumerically("==", 1))
			Expect(closeReq.ClosingReason()).To(Equal("time to go home"))
		})

		It("knows the bytes needed to encode itself", func() {
			closeReq := CloseRequest{
				correlationId: 127,
				closingCode:   255,
				closingReason: "bye",
			}
			Expect(closeReq.SizeNeeded()).To(BeNumerically("==", 15))

			closeReq.closingReason = "work is done"
			Expect(closeReq.SizeNeeded()).To(BeNumerically("==", 24))
		})

		It("encodes itself into a binary sequence", func() {
			closeReq := CloseRequest{
				correlationId: 255,
				closingCode:   127,
				closingReason: "bye",
			}
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)

			bytesWritten, err := closeReq.Write(wr)
			Expect(err).ToNot(HaveOccurred())
			Expect(wr.Flush()).To(Succeed())

			Expect(bytesWritten).To(BeNumerically("==", 11))

			expectedByteSequence := []byte{
				0x00, 0x00, 0x00, 0xff, // correlation id
				0x00, 0x7f, // closing code
				0x00, 0x03, // string len
			}
			expectedByteSequence = append(expectedByteSequence, []byte("bye")...)
			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})

		It("decodes a binary sequence into itself", func() {
			byteSequence := []byte{
				0x00, 0x00, 0x00, 0xff, // correlation id
				0x00, 0x7f, // closing code
				0x00, 0x07, // string len
			}
			byteSequence = append(byteSequence, []byte("success")...)

			closeReq := new(CloseRequest)
			Expect(closeReq.Read(bufio.NewReader(bytes.NewReader(byteSequence)))).To(Succeed())

			Expect(closeReq.correlationId).To(BeNumerically("==", 255))
			Expect(closeReq.closingCode).To(BeNumerically("==", 127))
			Expect(closeReq.closingReason).To(Equal("success"))
		})
	})
})
