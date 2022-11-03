package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tune", func() {
	Context("request", func() {
		It("decodes a binary sequence correctly", func() {
			tuneBinary := []byte{
				0x12, 0x34, 0x56, 0x78, // frame max
				0x98, 0x76, 0x54, 0x32, // heartbeat
			}
			tuneReq := new(TuneRequest)

			Expect(tuneReq.Read(bufio.NewReader(bytes.NewReader(tuneBinary)))).To(Succeed())
			Expect(tuneReq.frameMaxSize).To(BeNumerically("==", 305_419_896))
			Expect(tuneReq.heartbeatPeriod).To(BeNumerically("==", 2_557_891_634))
		})
	})

	Context("response", func() {
		It("has the expected attributes", func() {
			tuneReq := NewTuneResponse(1, 2)
			Expect(tuneReq.frameMaxSize).To(BeNumerically("==", 1))
			Expect(tuneReq.heartbeatPeriod).To(BeNumerically("==", 2))
			Expect(tuneReq.Key()).To(BeNumerically("==", 0x0014))
			Expect(tuneReq.Version()).To(BeNumerically("==", 1))
			Expect(tuneReq.SizeNeeded()).To(BeNumerically("==", 12))
		})

		It("binary encodes a struct", func() {
			tuneReq := NewTuneResponse(255, 127)
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)

			n, err := tuneReq.Write(wr)
			Expect(err).ToNot(HaveOccurred())
			Expect(n).To(BeNumerically("==", 8))
			Expect(wr.Flush()).To(Succeed())

			expectedByteSequence := []byte{0x00, 0x00, 0x00, 0xff, 0x00, 0x00, 0x00, 0x7f}
			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})
})
