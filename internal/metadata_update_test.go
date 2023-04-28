package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Metadata Update Response", func() {
	//MetadataUpdate => Key Version MetadataInfo
	//Key => uint16 // 0x0010
	//Version => uint16
	//MetadataInfo => Code Stream
	//Code => uint16 // code to identify the information
	//Stream => string // the stream implied

	It("decodes a binary sequence", func() {
		code := uint16(1)
		stream := "stream"
		buff := new(bytes.Buffer)
		wr := bufio.NewWriter(buff)
		_, err := writeMany(wr, code, stream)
		Expect(err).ToNot(HaveOccurred())
		Expect(wr.Flush()).To(Succeed())

		response := MetadataUpdateResponse{}
		Expect(response.Read(bufio.NewReader(buff))).To(Succeed())
		Expect(response.metadataInfo.code).To(BeNumerically("==", 1))
		Expect(response.metadataInfo.stream).To(Equal("stream"))
	})
})
