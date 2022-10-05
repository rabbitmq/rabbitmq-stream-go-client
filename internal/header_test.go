package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"io"
)

var _ = Describe("Header", func() {
	Context("Binary encoding/decoding", func() {
		var (
			someHeader = []byte{
				0x01, 0x02, 0x03, 0x04, // length/size
				0x10, 0x20, // key ID
				0x00, 0x05, // version
			}
			someHeaderRequest = Header{
				length:  123,
				command: 456,
				version: 5,
			}
		)

		It("unmarshal binary contents correctly", func() {
			aHeader := new(Header)
			Expect(aHeader.UnmarshalBinary(someHeader)).To(Succeed())
			Expect(aHeader.length).To(BeNumerically("==", 16909060))
			Expect(aHeader.command).To(BeNumerically("==", 4128))
			Expect(aHeader.version).To(BeNumerically("==", 5))
		})

		It("reads a binary buffer into a struct", func() {
			theHeader := new(Header)
			err := theHeader.Read(bufio.NewReader(bytes.NewReader(someHeader)))
			Expect(err).ToNot(HaveOccurred())
			Expect(theHeader.length).To(BeNumerically("==", 0x01020304))
			Expect(theHeader.Command()).To(BeNumerically("==", 0x1020))
			Expect(theHeader.Version()).To(BeNumerically("==", 0x0005))
		})

		When("the binary enconding is smaller than 8 bytes", func() {
			It("returns an error", func() {
				var notGoodHeader = []byte{0x12, 0x12, 0x12}
				emptyHeader := new(Header)
				err := emptyHeader.UnmarshalBinary(notGoodHeader)
				Expect(err).To(HaveOccurred())
				Expect(err).To(MatchError(io.ErrUnexpectedEOF))
			})
		})

		It("writes a binary format correctly", func() {
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			bytesWritten, err := someHeaderRequest.Write(wr)
			Expect(err).ToNot(HaveOccurred())

			By("writing the expected amount of bytes")
			Expect(bytesWritten).To(BeNumerically("==", 8))

			By("writing the expected byte sequence")
			Expect(wr.Flush()).To(Succeed())
			expectedBinaryHeader := []byte{
				0x00, 0x00, 0x00, 0x7B, // length
				0x01, 0xC8, // key ID
				0x00, 0x05, // version
			}
			Expect(buff.Bytes()).To(Equal(expectedBinaryHeader))
		})
	})
})
