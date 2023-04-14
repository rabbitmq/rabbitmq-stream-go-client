package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("StoreOffset", func() {
	//StoreOffset => Key Version Reference Stream Offset
	//Key => uint16 // 0x000a
	//Version => uint16
	//Reference => string // max 256 characters
	//Stream => string // the name of the stream
	//Offset => uint64

	Describe("StoreOffsetRequest", func() {
		It("returns the size needed to encode the frame", func() {
			storeOffsetRequest := NewStoreOffsetRequest("ref", "stream", uint64(1))

			expectedSize := 2 + 2 + // key ID + version
				2 + 3 + // uint16 for the reference string + uint32 for reference string length
				2 + 6 + // uint16 for the stream string + uint32 stream string length
				8 // uint64 offset
			Expect(storeOffsetRequest.SizeNeeded()).To(Equal(expectedSize))
		})

		It("has the required fields", func() {
			storeOffsetRequest := NewStoreOffsetRequest("ref", "stream", uint64(1))
			Expect(storeOffsetRequest.Key()).To((BeNumerically("==", 0x000a)))
			Expect(storeOffsetRequest.Version()).To((BeNumerically("==", 1)))
			Expect(storeOffsetRequest.Stream()).To(Equal("stream"))
			Expect(storeOffsetRequest.Reference()).To(Equal("ref"))
			Expect(storeOffsetRequest.Offset()).To(BeNumerically("==", 1))
		})

		It("can binary encode itself into a binary sequence", func() {
			storeOffsetRequest := NewStoreOffsetRequest("ref", "stream", uint64(1))
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			bytesWritten, err := storeOffsetRequest.Write(wr)

			Expect(err).NotTo(HaveOccurred())
			Expect(wr.Flush()).To(Succeed())
			Expect(bytesWritten).To(BeNumerically("==", storeOffsetRequest.SizeNeeded()-streamProtocolHeaderSizeBytes))

			expectedByteSequence := []byte{
				0x00, 0x03, byte('r'), byte('e'), byte('f'), // reference len + reference string
				0x00, 0x06, byte('s'), byte('t'), byte('r'), byte('e'), byte('a'), byte('m'), // reference len + reference string
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 1, // offset uint68
			}

			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})
})
