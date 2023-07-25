package internal

import (
	"bufio"
	"bytes"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/common"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/constants"
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

			// we simulate a message aggregation
			// that can be done by the client
			// First loop to aggregate the messages
			var fakeMessages []common.Serializer
			for i := 0; i < 5; i++ {
				fakeMessages = append(fakeMessages, NewFakeMessage(uint64(i), []byte(
					fmt.Sprintf("fake message %d", i))))
			}

			// It is time to prepare the buffer to send to the server
			for i := 0; i < len(fakeMessages); i++ {
				data, err := fakeMessages[i].MarshalBinary()
				Expect(err).ToNot(HaveOccurred())
				Expect(len(data)).To(BeNumerically("==", 8+4+len(fmt.Sprintf("fake message %d", i))))
				l, err := buffFakeMessagesBuffer.Write(data)
				Expect(err).ToNot(HaveOccurred())
				Expect(l).To(BeNumerically("==", 8+4+14))

			}

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
			Expect(batchPublishRequest.unCompressedDataSize).To(BeNumerically("==", 130))
			Expect(batchPublishRequest.compressedDataSize).To(BeNumerically("==", 130))
			Expect(batchPublishRequest.compressType).To(BeNumerically("==", 128)) // compressType = 0. 128 == 0x80 | compressType<<4
			Expect(batchPublishRequest.subBatchMessagesCount).To(BeNumerically("==", 5))
			Expect(batchPublishRequest.subBatchMessages).To(BeEquivalentTo(buffFakeMessagesBuffer.Bytes()))
			Expect(batchPublishRequest.publisherId).To(BeNumerically("==", 200))
			Expect(batchPublishRequest.numberOfRootMessages).To(BeNumerically("==", 1))
		})

		It("encodes fake message GZIP compression", func() {
			buffFakeMessagesBuffer := new(bytes.Buffer)

			// we simulate a message aggregation
			// that can be done by the client
			// First loop to aggregate the messages
			var fakeMessages []common.Serializer
			for i := 0; i < 5; i++ {
				fakeMessages = append(fakeMessages, NewFakeMessage(uint64(i), []byte(
					fmt.Sprintf("fake message %d", i))))
			}

			// It is time to prepare the buffer to send to the server
			for i := 0; i < len(fakeMessages); i++ {
				data, err := fakeMessages[i].MarshalBinary()
				Expect(err).ToNot(HaveOccurred())
				Expect(len(data)).To(BeNumerically("==", 8+4+len(fmt.Sprintf("fake message %d", i))))
				l, err := buffFakeMessagesBuffer.Write(data)
				Expect(err).ToNot(HaveOccurred())
				Expect(l).To(BeNumerically("==", 8+4+14))

			}

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
			Expect(batchPublishRequest.unCompressedDataSize).To(BeNumerically("==", 130))
			Expect(batchPublishRequest.compressedDataSize).To(BeNumerically("==", 70))
			Expect(batchPublishRequest.compressType).To(BeNumerically("==", 144)) // CompressionType = 1. 144 == 0x80 | compressType<<4
			Expect(batchPublishRequest.subBatchMessagesCount).To(BeNumerically("==", 5))
			Expect(batchPublishRequest.subBatchMessages).To(BeEquivalentTo(compress))
			Expect(batchPublishRequest.publisherId).To(BeNumerically("==", 200))
			Expect(batchPublishRequest.numberOfRootMessages).To(BeNumerically("==", 1))
		})

		It("encodes AMQP 1.0 message without compression", func() {
			buffAMQP10 := new(bytes.Buffer)

			// we simulate a message aggregation
			// that can be done by the client
			// First loop to aggregate the messages
			var amqpMessages []common.Serializer
			for i := 0; i < 5; i++ {
				amqpMessages = append(amqpMessages, amqp.NewAMQP10Message([]byte(
					fmt.Sprintf("AMQP_ message %d", i))))
			}

			// It is time to prepare the buffer to send to the server
			for i := 0; i < len(amqpMessages); i++ {
				data, err := amqpMessages[i].MarshalBinary()
				Expect(err).ToNot(HaveOccurred())
				Expect(len(data)).To(BeNumerically("==", 3+ // Format code from AMQP 1.0 3 bytes
					2+ // 1 byte to define the application data types (AMQP 1.0) byte(typeCodeVbin8)
					// 1 byte to the length of the message byte(15)
					// So the total is 5 bytes + the length of the message
					len(fmt.Sprintf("AMQP_ message %d", i))))

				l, err := buffAMQP10.Write(data)
				Expect(err).ToNot(HaveOccurred())
				Expect(l).To(BeNumerically("==", 3+2+15))
			}

			compressedNone := &common.CompressNONE{}

			compress, err := compressedNone.Compress(buffAMQP10.Bytes())
			Expect(err).ToNot(HaveOccurred())

			// in this case we don't compress so bytes must be the same
			Expect(compress).To(BeEquivalentTo(buffAMQP10.Bytes()))

			batchPublishRequest := NewSubBatchPublishRequest(200,
				1,
				100,
				constants.CompressionNone,
				uint16(len(amqpMessages)),
				buffAMQP10.Len(),   // uncompressed size
				len(compress),      // compressed size
				buffAMQP10.Bytes(), // subBatchMessages bytes
			)
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			Expect(batchPublishRequest.Write(wr)).To(BeNumerically("==", batchPublishRequest.SizeNeeded()-streamProtocolHeaderSizeBytes))
			Expect(wr.Flush()).To(Succeed())
			Expect(batchPublishRequest.unCompressedDataSize).To(BeNumerically("==", 100)) // 20 * 5
			Expect(batchPublishRequest.compressedDataSize).To(BeNumerically("==", 100))   // 20 * 5
			Expect(batchPublishRequest.compressType).To(BeNumerically("==", 128))         // compressType = 0. 128 == 0x80 | compressType<<4
			Expect(batchPublishRequest.subBatchMessagesCount).To(BeNumerically("==", 5))
			Expect(batchPublishRequest.subBatchMessages).To(BeEquivalentTo(buffAMQP10.Bytes()))
			Expect(batchPublishRequest.publisherId).To(BeNumerically("==", 200))
			Expect(batchPublishRequest.numberOfRootMessages).To(BeNumerically("==", 1))
		})

		It("encodes AMQP 1.0 message with GZIP compression", func() {
			buffAMQP10 := new(bytes.Buffer)

			// we simulate a message aggregation
			// that can be done by the client
			// First loop to aggregate the messages
			var amqpMessages []common.Serializer
			// I don't know chinese, so I just copied the first paragraph of the wikipedia page for Alan Turing
			// https://zh.wikipedia.org/wiki/%E8%89%BE%E4%BC%A6%C2%B7%E5%9B%BE%E7%81%B5
			chString := "艾伦·麦席森·图灵，OBE，FRS（英語：Alan Mathison Turing，又译阿兰·图灵，Turing也常翻譯成涂林或者杜林，1912年6月23日—1954年6月7日）是英国電腦科學家、数学家、邏輯學家、密码分析学家和理论生物学家，他被誉为计算机科学與人工智能之父。\n\n二次世界大战期间，「Hut 8」小组，负责德国海军密码分析。 期间他设计了一些加速破译德国密码的技术，包括改进波兰战前研制的机器Bombe，一种可以找到恩尼格玛密码机设置的机电机器。 图灵在破译截获的编码信息方面发挥了关键作用，使盟军能够在包括大西洋战役在内的许多重要交战中击败軸心國海軍，并因此帮助赢得了战争[3][4]。"
			for i := 0; i < 5; i++ {
				amqpMessages = append(amqpMessages, amqp.NewAMQP10Message([]byte(
					fmt.Sprintf("%s %d", chString, i))))
			}

			// It is time to prepare the buffer to send to the server
			for i := 0; i < len(amqpMessages); i++ {
				data, err := amqpMessages[i].MarshalBinary()
				Expect(err).ToNot(HaveOccurred())
				Expect(len(data)).To(BeNumerically("==", 3+ // Format code from AMQP 1.0 3 bytes
					5+ // 1 byte to define the application data types (AMQP 1.0)
					// 4 bytes to define the length of the message
					//1 byte for (byte(typeCodeVbin32))
					//4 bytes for (uint32(l))
					// So the total is 5 bytes + the length of the message
					len(fmt.Sprintf("%s %d", chString, i))))
				l, err := buffAMQP10.Write(data)
				Expect(err).ToNot(HaveOccurred())
				Expect(l).To(BeNumerically("==", 783))

			}

			compressedGZIP := &common.CompressGZIP{}

			compress, err := compressedGZIP.Compress(buffAMQP10.Bytes())
			Expect(err).ToNot(HaveOccurred())

			batchPublishRequest := NewSubBatchPublishRequest(200,
				1,
				100,
				constants.CompressionGzip,
				uint16(len(amqpMessages)),
				buffAMQP10.Len(), // uncompressed size
				len(compress),    // compressed size
				compress,         // subBatchMessages bytes
			)
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			Expect(batchPublishRequest.Write(wr)).To(BeNumerically("==", batchPublishRequest.SizeNeeded()-streamProtocolHeaderSizeBytes))
			Expect(wr.Flush()).To(Succeed())
			Expect(batchPublishRequest.unCompressedDataSize).To(BeNumerically("==", 3915))
			Expect(batchPublishRequest.compressedDataSize).To(BeNumerically("==", 686))
			Expect(batchPublishRequest.compressType).To(BeNumerically("==", 144)) // compressType = 0. 128 == 0x80 | compressType<<4
			Expect(batchPublishRequest.subBatchMessagesCount).To(BeNumerically("==", 5))
			Expect(batchPublishRequest.subBatchMessages).To(BeEquivalentTo(compress))
			Expect(batchPublishRequest.publisherId).To(BeNumerically("==", 200))
			Expect(batchPublishRequest.numberOfRootMessages).To(BeNumerically("==", 1))
		})
	})
})
