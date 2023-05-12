package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Heartbeat", func() {
	var hb *Heartbeat
	BeforeEach(func() {
		hb = NewHeartbeat()
	})

	It("returns the size needed to encode itself", func() {
		Expect(hb.SizeNeeded()).To(BeNumerically("==", 4))
	})

	It("has the expected fields", func() {
		Expect(hb.Key()).To(BeNumerically("==", 0x0017))
		Expect(hb.Version()).To(BeNumerically("==", 1))
	})

	It("can encode itself into a binary sequence", func() {
		buff := new(bytes.Buffer)
		wr := bufio.NewWriter(buff)
		Expect(hb.Write(wr)).To(BeNumerically("==", hb.SizeNeeded()))
		Expect(wr.Flush()).To(Succeed())

		expectedByteSequence := []byte{
			0x00, 0x17, // commandId
			0x00, 0x01, // version
		}

		Expect(buff.Bytes()).To(Equal(expectedByteSequence))
	})

	It("can decode itself from a binary sequence", func() {
		byteSequence := []byte{
			0x00, 0x17, // commandId
			0x00, 0x01, // version
		}
		Expect(hb.Read(bufio.NewReader(bytes.NewReader(byteSequence)))).To(Succeed())
		Expect(hb.command).To(BeNumerically("==", 23))
		Expect(hb.version).To(BeNumerically("==", 1))
	})
})
