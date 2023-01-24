package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ExchangeCommandVersions", func() {
	It("encodes itself into a binary sequence", func() {
		exchangeCommand := NewExchangeCommandVersionsRequestWith(123, []commandInformation{
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
