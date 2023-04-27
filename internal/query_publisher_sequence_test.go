package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("QueryPublisherSequence", func() {

	//QueryPublisherRequest => Key Version CorrelationId PublisherReference Stream
	//Key => uint16 // 0x0005
	//Version => uint16
	//CorrelationId => uint32
	//PublisherReference => string // max 256 characters
	//Stream => string
	Describe("Request", func() {
		It("returns the expected attributes", func() {
			queryPubSubReq := NewQueryPublisherSequenceRequest("pubref", "stream")
			queryPubSubReq.SetCorrelationId(3)

			Expect(queryPubSubReq.Key()).To(BeNumerically("==", 0x0005))
			Expect(queryPubSubReq.Version()).To(BeNumerically("==", 1))
			Expect(queryPubSubReq.CorrelationId()).To(BeNumerically("==", 3))
			Expect(queryPubSubReq.PublisherReference()).To(Equal("pubref"))
			Expect(queryPubSubReq.Stream()).To(Equal("stream"))
		})

		// TODO: where do we enforce strings are max 256 characters?
		It("can calculate the size needed to binary encode itself", func() {
			queryPubSubReq := NewQueryPublisherSequenceRequest("pubref", "stream")
			queryPubSubReq.SetCorrelationId(3)
			expectedSize := 2 + 2 + // keyId + Version
				4 + // uint32 for correlationID
				2 + 6 + // uint16 for publisherReference string + uint32 stream string length
				2 + 6 // uint16 for stream string + uint32 stream string length
			Expect(queryPubSubReq.SizeNeeded()).To(Equal(expectedSize))
		})
	})

	//QueryPublisherResponse => Key Version CorrelationId ResponseCode Sequence
	//Key => uint16 // 0x8005
	//Version => uint16
	//CorrelationId => uint32
	//ResponseCode => uint16
	//Sequence => uint64
	Describe("Response", func() {
		It("decodes a binary sequence into itself", func() {
			queryPublisherResponse := NewQueryPublisherSequenceResponse()
			binaryQueryPubSub := []byte{

				0x00, 0x00, 0x00, 0x03, // correlation id
				0x00, 0x01, // responseCode
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A,
			}

			buff := bytes.NewBuffer(binaryQueryPubSub)
			reader := bufio.NewReader(buff)
			Expect(queryPublisherResponse.Read(reader)).To(Succeed())

			Expect(queryPublisherResponse.CorrelationId()).To(BeNumerically("==", 3))
			Expect(queryPublisherResponse.ResponseCode()).To(BeNumerically("==", 1))
			Expect(queryPublisherResponse.Sequence()).To(BeNumerically("==", 42))
		})

	})
})
