package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("SaslMechanisms", func() {
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
