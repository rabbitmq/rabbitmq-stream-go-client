package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Create", func() {
	Context("Request", func() {
		It("returns the expected attributes", func() {
			seq := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64} // publishingId sequence number 100
			seq = append(seq, []byte{0x00, 0x00, 0x00, 0x03}...)          //message size
			seq = append(seq, []byte("foo")...)                           // message body

			createRequest := NewPublishRequest(17, 1, seq)
			Expect(createRequest.Key()).To(BeNumerically("==", 0x0002))
			Expect(createRequest.Version()).To(BeNumerically("==", 1))

			expectedSize := streamProtocolKeySizeBytes +
				streamProtocolVersionSizeBytes +
				streamProtocolKeySizeUint8 + // publisherId
				streamProtocolKeySizeUint32 + // message count
				+len(seq) // message body

			Expect(createRequest.SizeNeeded()).To(BeNumerically("==", expectedSize))
		})

		It("encodes itself into a binary sequence", func() {
			seq := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64} // publishingId sequence number 100
			seq = append(seq, []byte{0x00, 0x00, 0x00, 0x03}...)          //message size
			seq = append(seq, []byte("foo")...)                           // message body

			c := NewPublishRequest(17, 1, seq)
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			Expect(c.Write(wr)).To(BeNumerically("==", c.SizeNeeded()-streamProtocolHeaderSizeBytes))
			Expect(wr.Flush()).To(Succeed())

			expectedByteSequence := []byte{
				0x11,                   // publisher ID 17
				0x00, 0x00, 0x00, 0x01, // message count 1

			}
			expectedByteSequence = append(expectedByteSequence, seq...)
			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})

		It("encodes fake message", func() {
			buffFakeMessages := new(bytes.Buffer)
			writerFakeMessages := bufio.NewWriter(buffFakeMessages)

			// we simulate a message aggregation
			// that can be done by the client
			// First loop to aggregate the messages
			var fakeMessages []costants.StreamerMessage
			fakeMessages = append(fakeMessages, NewFakeMessage(17, []byte("foo")))
			fakeMessages = append(fakeMessages, NewFakeMessage(21, []byte("wine")))
			fakeMessages = append(fakeMessages, NewFakeMessage(23, []byte("beer")))
			fakeMessages = append(fakeMessages, NewFakeMessage(29, []byte("water")))

			// It is time to prepare the buffer to send to the server
			for _, fakeMessage := range fakeMessages {
				Expect(fakeMessage.Write(writerFakeMessages)).To(BeNumerically("==", 8+4+len(fakeMessage.GetBody())))
			}
			Expect(writerFakeMessages.Flush()).To(Succeed())

			c := NewPublishRequest(200, uint32(len(fakeMessages)), buffFakeMessages.Bytes())
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			Expect(c.Write(wr)).To(BeNumerically("==", c.SizeNeeded()-streamProtocolHeaderSizeBytes))
			Expect(wr.Flush()).To(Succeed())

		})

	})

	Context("Response", func() {
		It("decodes a binary sequence", func() {
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			_, err := writeMany(wr, uint32(12345), uint16(101))
			Expect(err).ToNot(HaveOccurred())
			Expect(wr.Flush()).To(Succeed())

			response := SimpleResponse{}
			Expect(response.Read(bufio.NewReader(buff))).To(Succeed())

			Expect(response.correlationId).To(BeNumerically("==", 12345))
			Expect(response.responseCode).To(BeNumerically("==", 101))
		})
	})
})
