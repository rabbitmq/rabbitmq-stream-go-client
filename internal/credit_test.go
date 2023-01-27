package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Internal/Credit", func() {
	Context("Request", func() {
		It("knows the size needed to encode itself", func() {
			c := &CreditRequest{
				subscriptionId: 123,
				credit:         987,
			}
			Expect(c.SizeNeeded()).To(BeNumerically("==", streamProtocolHeaderSizeBytes+1+2))
		})

		It("encodes itself into a binary sequence", func() {
			c := &CreditRequest{
				subscriptionId: 5,
				credit:         255,
			}
			buff := &bytes.Buffer{}
			wr := bufio.NewWriter(buff)
			Expect(c.Write(wr)).To(BeNumerically("==", 3))
			Expect(wr.Flush()).To(Succeed())

			expectedByteSequence := []byte{
				0x05,       // subscription ID
				0x00, 0xff, // credit
			}
			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})

	Context("Response", func() {
		It("decodes itself into a response struct", func() {
			byteSequence := []byte{
				0x00, 0x0f, // response code
				0x10, // sub ID
			}

			buff := bytes.NewBuffer(byteSequence)
			r := bufio.NewReader(buff)
			c := &CreditResponse{}

			Expect(c.Read(r)).Should(Succeed())
			Expect(c.responseCode).To(BeNumerically("==", 15))
			Expect(c.subscriptionId).To(BeNumerically("==", 16))
		})
	})
})
