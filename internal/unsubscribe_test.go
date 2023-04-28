package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Unsubscribe", func() {
	//Unsubscribe => Key Version CorrelationId SubscriptionId
	//Key => uint16 // 0x000c
	//Version => uint16
	//CorrelationId => uint32
	//SubscriptionId => uint8

	It("has the required fields", func() {
		unSub := NewUnsubscribe(1)
		Expect(unSub.Key()).To(BeNumerically("==", 0x000c))
		Expect(unSub.Version()).To(BeNumerically("==", 1))
	})

	It("returns the size needed to encode the frame", func() {
		unSub := NewUnsubscribe(1)
		unSub.SetCorrelationId(3)

		expectedSize := 2 + 2 + // key ID + version
			4 + // correlationID
			1 // uint8 for subscriptionId
		Expect(unSub.SizeNeeded()).To(Equal(expectedSize))
	})

	It("can encode itself into a binary sequence", func() {
		unSub := NewUnsubscribe(1)
		unSub.SetCorrelationId(3)

		buff := new(bytes.Buffer)
		wr := bufio.NewWriter(buff)
		bytesWritten, err := unSub.Write(wr)

		Expect(err).NotTo(HaveOccurred())
		Expect(wr.Flush()).To(Succeed())
		Expect(bytesWritten).To(BeNumerically(
			"==", unSub.SizeNeeded()-streamProtocolHeaderSizeBytes))

		expectedByteSequence := []byte{
			0x00, 0x00, 0x00, 0x03, // correlationId
			0x01, // uint8 subscriptionId
		}

		Expect(buff.Bytes()).To(Equal(expectedByteSequence))
	})
})
