package internal

import (
	"bufio"
	"bytes"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/common"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Create", func() {
	Context("SubBatch Entry Request", func() {

		It("returns the expected sub batch messages bytes", func() {
			subBatch := []byte{0x00, 0x00, 0x00, 0x03}    //message size
			subBatch = append(subBatch, []byte("foo")...) // message content

			createRequest := NewSubBatchPublishRequest(4, 1,
				100, 1, 1, 3+4, 3+4,
				subBatch)
			Expect(createRequest.Key()).To(BeNumerically("==", 0x0002))
			Expect(createRequest.Version()).To(BeNumerically("==", 1))
			Expect(createRequest.publisherId).To(BeNumerically("==", 4))
			Expect(createRequest.numberOfRootMessages).To(BeNumerically("==", 1))
			Expect(createRequest.publishingId).To(BeNumerically("==", 100))
			Expect(createRequest.compressType).To(BeNumerically("==", 144 /* 0x80 | compressType<<4 */))
			Expect(createRequest.subBatchMessagesCount).To(BeNumerically("==", 1))
			Expect(createRequest.unCompressedDataSize).To(BeNumerically("==", 3+4))
			Expect(createRequest.compressedDataSize).To(BeNumerically("==", 4+3))
			Expect(createRequest.subBatchMessages).To(BeEquivalentTo(subBatch))

		})

		It("returns the expected full bytes request", func() {
			seq := []byte{0x04}                                                          // publisherId
			seq = append(seq, []byte{0x00, 0x00, 0x00, 0x01}...)                         //numberOfRootMessages 1
			seq = append(seq, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64}...) //publishingId sequence number 100
			seq = append(seq, []byte{0x90}...)                                           //compressType 1 144 == 0x80 | compressType<<4
			seq = append(seq, []byte{0x00, 0x01}...)                                     //subBatchMessagesCount 1
			seq = append(seq, []byte{0x00, 0x00, 0x00, 0x07}...)                         //unCompressedDataSize 3
			seq = append(seq, []byte{0x00, 0x00, 0x00, 0x07}...)                         //compressedDataSize 3
			subBatch := []byte{0x00, 0x00, 0x00, 0x03}                                   //message size
			subBatch = append(subBatch, []byte("foo")...)                                // message content
			seq = append(seq, subBatch...)

			createRequest := NewSubBatchPublishRequest(4, 1,
				100, 1, 1, 3+4, 3+4,
				subBatch)

			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			Expect(createRequest.Write(wr)).To(BeNumerically("==", createRequest.SizeNeeded()-streamProtocolHeaderSizeBytes))
			Expect(wr.Flush()).To(Succeed())
			Expect(buff.Bytes()).To(BeEquivalentTo(seq))
		})

		It("check size needed", func() {
			subBatch := []byte{0x00, 0x00, 0x00, 0x03}    //message size
			subBatch = append(subBatch, []byte("foo")...) // message content

			createRequest := NewSubBatchPublishRequest(4, 1,
				100, 0, 1, 3+4, 3+4,
				subBatch)
			expectedSize := streamProtocolKeySizeBytes +
				streamProtocolVersionSizeBytes +
				streamProtocolKeySizeUint8 + // publisherId
				streamProtocolKeySizeUint32 + // numberOfRootMessages
				streamProtocolKeySizeUint64 + //publishingId
				streamProtocolKeySizeUint8 + // compressType
				streamProtocolKeySizeUint16 + // subEntryMessagesCount
				streamProtocolKeySizeUint32 + // uncompressedDataSize
				streamProtocolKeySizeUint32 + // compressedDataSize
				4 + 3 // subBatchMessages size

			Expect(createRequest.SizeNeeded()).To(BeNumerically("==", expectedSize))
		})

		It("encodes fake message without compression", func() {
			buffFakeMessagesBuffer := new(bytes.Buffer)
			writerFakeMessagesWriter := bufio.NewWriter(buffFakeMessagesBuffer)

			// we simulate a message aggregation
			// that can be done by the client
			// First loop to aggregate the messages
			var fakeMessages []common.StreamerMessage
			fakeMessages = append(fakeMessages, NewFakeMessage(17, []byte("foo")))
			fakeMessages = append(fakeMessages, NewFakeMessage(21, []byte("wine")))
			fakeMessages = append(fakeMessages, NewFakeMessage(23, []byte("beer")))
			fakeMessages = append(fakeMessages, NewFakeMessage(29, []byte("water")))

			// It is time to prepare the buffer to send to the server
			for _, fakeMessage := range fakeMessages {
				Expect(fakeMessage.WriteTo(writerFakeMessagesWriter)).To(BeNumerically("==", 8+4+len(fakeMessage.Body())))
			}
			Expect(writerFakeMessagesWriter.Flush()).To(Succeed())

			compressedNone := &common.CompressNONE{}

			compress, err := compressedNone.Compress(buffFakeMessagesBuffer.Bytes())
			Expect(err).ToNot(HaveOccurred())

			// in this case we don't compress so bytes must be the same
			Expect(compress).To(BeEquivalentTo(buffFakeMessagesBuffer.Bytes()))

			batchPublishRequest := NewSubBatchPublishRequest(200,
				1,
				100,
				constants.CompressionNone,
				uint16(len(fakeMessages)),
				buffFakeMessagesBuffer.Len(),   // uncompressed size
				len(compress),                  // compressed size
				buffFakeMessagesBuffer.Bytes(), // subBatchMessages bytes
			)
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			Expect(batchPublishRequest.Write(wr)).To(BeNumerically("==", batchPublishRequest.SizeNeeded()-streamProtocolHeaderSizeBytes))
			Expect(wr.Flush()).To(Succeed())
			Expect(buffFakeMessagesBuffer.Len()).To(BeNumerically("==", batchPublishRequest.unCompressedDataSize))
			Expect(buffFakeMessagesBuffer.Len()).To(BeNumerically("==", batchPublishRequest.compressedDataSize))
			Expect(batchPublishRequest.compressType).To(BeNumerically("==", 128)) // compressType = 0. 128 == 0x80 | compressType<<4
			Expect(batchPublishRequest.subBatchMessagesCount).To(BeNumerically("==", len(fakeMessages)))
			Expect(batchPublishRequest.subBatchMessages).To(BeEquivalentTo(buffFakeMessagesBuffer.Bytes()))
			Expect(batchPublishRequest.publisherId).To(BeNumerically("==", 200))
			Expect(batchPublishRequest.numberOfRootMessages).To(BeNumerically("==", 1))
		})

		It("encodes fake message GZIP compression", func() {
			buffFakeMessagesBuffer := new(bytes.Buffer)
			writerFakeMessagesWriter := bufio.NewWriter(buffFakeMessagesBuffer)

			// we simulate a message aggregation
			// that can be done by the client
			// First loop to aggregate the messages
			var fakeMessages []common.StreamerMessage
			fakeMessages = append(fakeMessages, NewFakeMessage(17, []byte("foo")))
			fakeMessages = append(fakeMessages, NewFakeMessage(21, []byte("wine")))
			fakeMessages = append(fakeMessages, NewFakeMessage(23, []byte("beer")))
			fakeMessages = append(fakeMessages, NewFakeMessage(29, []byte("water")))

			// It is time to prepare the buffer to send to the server
			for _, fakeMessage := range fakeMessages {
				Expect(fakeMessage.WriteTo(writerFakeMessagesWriter)).To(BeNumerically("==", 8+4+len(fakeMessage.Body())))
			}
			Expect(writerFakeMessagesWriter.Flush()).To(Succeed())

			compressedGZIP := &common.CompressGZIP{}

			compress, err := compressedGZIP.Compress(buffFakeMessagesBuffer.Bytes())
			Expect(err).ToNot(HaveOccurred())

			batchPublishRequest := NewSubBatchPublishRequest(200,
				1,
				101,
				constants.CompressionGzip,
				uint16(len(fakeMessages)),
				buffFakeMessagesBuffer.Len(), // uncompressed size
				len(compress),                // compressed size
				compress,                     // subBatchMessages compressed bytes
			)
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			Expect(batchPublishRequest.Write(wr)).To(BeNumerically("==", batchPublishRequest.SizeNeeded()-streamProtocolHeaderSizeBytes))
			Expect(wr.Flush()).To(Succeed())
			Expect(buffFakeMessagesBuffer.Len()).To(BeNumerically("==", batchPublishRequest.unCompressedDataSize))
			Expect(len(compress)).To(BeNumerically("==", batchPublishRequest.compressedDataSize))
			Expect(batchPublishRequest.compressType).To(BeNumerically("==", 144)) // CompressionType = 1. 144 == 0x80 | compressType<<4
			Expect(batchPublishRequest.subBatchMessagesCount).To(BeNumerically("==", len(fakeMessages)))
			Expect(batchPublishRequest.subBatchMessages).To(BeEquivalentTo(compress))
			Expect(batchPublishRequest.publisherId).To(BeNumerically("==", 200))
			Expect(batchPublishRequest.numberOfRootMessages).To(BeNumerically("==", 1))
		})
	})
})
