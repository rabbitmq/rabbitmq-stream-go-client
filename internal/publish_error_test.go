package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Publish Error Response", func() {
	//PublishError => Key Version [PublishingError]
	//Key => uint16 // 0x0004
	//Version => uint16
	//PublisherId => uint8
	//PublishingError => PublishingId Code
	//PublishingId => uint64
	//Code => uint16 // code to identify the problem

	It("has the required fields", func() {
		pubErr := NewPublishErrorResponse(1, []PublishingError{{42, 8}})
		Expect(pubErr.Key()).To(BeNumerically("==", 0x0004))
		Expect(pubErr.MinVersion()).To(BeNumerically("==", 1))
		Expect(pubErr.MaxVersion()).To(BeNumerically("==", 1))
		Expect(pubErr.PublisherId()).To(BeNumerically("==", 1))

	})

	It("decodes a binary sequence", func() {
		buff := new(bytes.Buffer)
		wr := bufio.NewWriter(buff)
		_, err := writeMany(wr, uint8(4), uint32(2), uint64(42), uint16(1), uint64(43), uint16(2))
		Expect(err).ToNot(HaveOccurred())
		Expect(wr.Flush()).To(Succeed())

		response := PublishErrorResponse{}
		Expect(response.Read(bufio.NewReader(buff))).To(Succeed())

		Expect(response.publisherId).To(BeNumerically("==", 4))
		Expect(response.publishingErrors[0].publishingId).To(BeNumerically("==", 42))
		Expect(response.publishingErrors[0].code).To(BeNumerically("==", 1))
		Expect(response.publishingErrors[1].publishingId).To(BeNumerically("==", 43))
		Expect(response.publishingErrors[1].code).To(BeNumerically("==", 2))
	})
})
