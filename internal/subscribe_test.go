package internal

import (
	"bufio"
	"bytes"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Subscribe", func() {
	Context("Request", func() {
		It("returns the expected attributes", func() {
			subscribeRequest := NewSubscribeRequestRequest(12, "mystream",
				constants.OffsetTypeLast, 60_001, 5,
				map[string]string{"some-config": "it-works"})
			Expect(subscribeRequest.Key()).To(BeNumerically("==", 0x0007))
			Expect(subscribeRequest.Version()).To(BeNumerically("==", 1))
			Expect(subscribeRequest.offsetType).To(BeNumerically("==", 2))
			Expect(subscribeRequest.offset).To(BeNumerically("==", 60_001))
			Expect(subscribeRequest.credit).To(BeNumerically("==", 5))
			Expect(subscribeRequest.stream).To(Equal("mystream"))
			Expect(subscribeRequest.properties).To(HaveKeyWithValue("some-config", "it-works"))

			subscribeRequest.SetCorrelationId(42346)
			Expect(subscribeRequest.CorrelationId()).To(BeNumerically("==", 42346))

			expectedSize := streamProtocolHeader +
				streamProtocolKeySizeUint8 + // subscriptionId id
				streamProtocolStringLenSizeBytes + len("mystream") + // stream
				streamProtocolKeySizeUint16 + // offsetType
				streamProtocolKeySizeUint64 + // offset
				streamProtocolKeySizeUint16 + // credit
				sizeNeededForMap(map[string]string{"some-config": "it-works"})

			Expect(subscribeRequest.SizeNeeded()).To(BeNumerically("==", expectedSize))
		})

		It("encodes itself into a binary sequence", func() {
			c := NewSubscribeRequestRequest(12, "mystream",
				constants.OffsetTypeFirst, 60_001, 5,
				map[string]string{"myarg": "myvalue"})
			c.SetCorrelationId(71)

			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			Expect(c.Write(wr)).To(BeNumerically("==", c.SizeNeeded()-streamProtocolHeaderSizeBytes))
			Expect(wr.Flush()).To(Succeed())

			expectedByteSequence := []byte{
				0x00, 0x00, 0x00, 0x47, // correlation ID
			}
			expectedByteSequence = append(expectedByteSequence, []byte{0x0c}...)                                           // subscriptionId
			expectedByteSequence = append(expectedByteSequence, []byte{0x00, 0x08}...)                                     // stream length
			expectedByteSequence = append(expectedByteSequence, []byte("mystream")...)                                     // stream name
			expectedByteSequence = append(expectedByteSequence, []byte{0x00, 0x01}...)                                     // offsetType
			expectedByteSequence = append(expectedByteSequence, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEA, 0x61}...) // offset value
			expectedByteSequence = append(expectedByteSequence, []byte{0x00, 0x05}...)                                     // credit
			expectedByteSequence = append(expectedByteSequence, []byte{0x00, 0x00, 0x00, 0x01}...)                         // map size
			expectedByteSequence = append(expectedByteSequence, 0x00, 0x05)                                                // myarg length
			expectedByteSequence = append(expectedByteSequence, []byte("myarg")...)
			expectedByteSequence = append(expectedByteSequence, 0x00, 0x07) // myvalue length
			expectedByteSequence = append(expectedByteSequence, []byte("myvalue")...)
			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})
})
