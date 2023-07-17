package internal

import (
	"bufio"
	"bytes"
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/common"
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

			// we simulate a message aggregation
			// that can be done by the client
			// First loop to aggregate the messages
			var fakeMessages []common.Message
			for i := 0; i < 5; i++ {
				fakeMessages = append(fakeMessages, NewFakeMessage(uint64(i), []byte(
					fmt.Sprintf("fake message %d", i))))
			}

			// It is time to prepare the buffer to send to the server
			for i := 0; i < len(fakeMessages); i++ {
				data, err := fakeMessages[i].MarshalBinary()
				Expect(err).ToNot(HaveOccurred())
				l, err := buffFakeMessages.Write(data)
				Expect(l).To(BeNumerically("==", 26))
				Expect(err).ToNot(HaveOccurred())
				Expect(len(data)).To(BeNumerically("==", 8+4+len(fmt.Sprintf("fake message %d", i))))
			}

			c := NewPublishRequest(200, uint32(len(fakeMessages)), buffFakeMessages.Bytes())
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			Expect(c.Write(wr)).To(BeNumerically("==", c.SizeNeeded()-streamProtocolHeaderSizeBytes))
			Expect(c.publisherId).To(BeNumerically("==", 200))
			Expect(c.messageCount).To(BeNumerically("==", 5))
			Expect(c.messages).To(HaveLen(130)) // 5 messages * 26 bytes each))
			Expect(wr.Flush()).To(Succeed())
		})
	})

	It("encodes AMQP1.0 message", func() {
		buffAMQP10 := new(bytes.Buffer)

		// we simulate a message aggregation
		// that can be done by the client
		// First loop to aggregate the messages
		// In this case we use AMQP1.0 message
		var amqpMessages []common.Message
		for i := 0; i < 5; i++ {
			amqpMessages = append(amqpMessages, amqp.NewAMQP10Message([]byte(
				fmt.Sprintf("AMQP message %d", i))))
		}

		// It is time to prepare the buffer to send to the server
		for i := 0; i < len(amqpMessages); i++ {
			data, err := amqpMessages[i].MarshalBinary()
			Expect(err).ToNot(HaveOccurred())

			l, err := buffAMQP10.Write(data)
			Expect(err).ToNot(HaveOccurred())
			Expect(l).To(BeNumerically("==", 19))

			Expect(len(data)).To(BeNumerically("==", 3+ // Format code from AMQP 1.0 3 bytes
				2+ // 2 bytes to define the application data types (AMQP 1.0)
				// 		[]byte{  byte(typeCodeVbin8),
				//			byte(15),
				//		})
				// So the total is 5 bytes + the length of the message
				len(fmt.Sprintf("AMQP message %d", i))))
		}

		c := NewPublishRequest(200, uint32(len(amqpMessages)), buffAMQP10.Bytes())
		buff := new(bytes.Buffer)
		wr := bufio.NewWriter(buff)
		Expect(c.Write(wr)).To(BeNumerically("==", c.SizeNeeded()-streamProtocolHeaderSizeBytes))
		Expect(c.publisherId).To(BeNumerically("==", 200))
		Expect(c.messageCount).To(BeNumerically("==", 5))
		Expect(c.messageCount).To(BeNumerically("==", 5))
		Expect(c.messages).To(HaveLen(95)) // 5 messages * 19 bytes each (14 + 2 +3 ))
		Expect(wr.Flush()).To(Succeed())
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
