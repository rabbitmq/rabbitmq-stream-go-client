package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Publish Confirm Response", func() {
	Context("Response", func() {
		It("decodes a binary sequence", func() {
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			_, err := writeMany(wr, uint8(12), uint32(3), uint64(12345), uint64(12346), uint64(12347))
			Expect(err).ToNot(HaveOccurred())
			Expect(wr.Flush()).To(Succeed())

			response := PublishConfirmResponse{}
			Expect(response.Read(bufio.NewReader(buff))).To(Succeed())

			Expect(response.publisherID).To(BeNumerically("==", 12))
			Expect(len(response.publishingIds)).To(BeNumerically("==", 3))
			Expect(response.publishingIds).To(HaveLen(3))
			Expect(response.publishingIds[0]).To(BeNumerically("==", 12345))
			Expect(response.publishingIds[1]).To(BeNumerically("==", 12346))
			Expect(response.publishingIds[2]).To(BeNumerically("==", 12347))
		})
	})
})
