package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ExchangeCommandVersions", func() {
	Context("request", func() {
		It("encodes itself into a binary sequence", func() {
			exchangeCommand := NewExchangeCommandVersionsRequestWith(123, []commandInformer{
				&ChunkResponse{},
			})

			buff := &bytes.Buffer{}
			wr := bufio.NewWriter(buff)
			Expect(exchangeCommand.Write(wr)).
				To(BeNumerically("==", exchangeCommand.SizeNeeded()-streamProtocolHeaderSizeBytes))
			Expect(wr.Flush()).To(Succeed())

			expectedByteSequence := []byte{
				0x00, 0x00, 0x00, 0x7b, // correlation id
				0x00, 0x00, 0x00, 0x01, // slice len
				0x00, 0x08, // command ID
				0x00, 0x02, // min version
				0x00, 0x02, // max version
			}
			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})

	Context("response",  func() {
		It("reads a response from a binary sequence", func() {
			binaryFrame := []byte{
				0x00, 0x00, 0x00, 0xff, // correlation id
				0x00, 0x7f, // response code
				0x00, 0x00, 0x00, 0x02, // slice len
				0x00, 0x0f, // element 1
				0x00, 0x01,
				0x00, 0x0a,
				0x00, 0x10, // element 2
				0x00, 0x05,
				0x00, 0x11,
			}

			response := &ExchangeCommandVersionsResponse{}
			Expect(response.Read(bufio.NewReader(bytes.NewBuffer(binaryFrame)))).To(Succeed())

			Expect(response.commands).To(HaveLen(2))
			Expect(response.CorrelationId()).To(BeNumerically("==", 255))
			//Expect(response.commands).To()
		})
	})
})
