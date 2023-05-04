package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ConsumerUpdate", func() {
	Context("Request", func() {
		//ConsumerUpdateQuery => Key Version CorrelationId SubscriptionId Active
		//Key => uint16 // 0x001a
		//Version => uint16
		//CorrelationId => uint32
		//SubscriptionId => uint8
		//Active => uint8 (boolean, 0 = false, 1 = true)

		It("has the expected fields", func() {
			req := NewConsumerUpdateQuery(42, 1)
			req.SetCorrelationId(42)

			Expect(req.Key()).To(BeNumerically("==", 0x001a))
			Expect(req.Version()).To(BeNumerically("==", 1))
			Expect(req.CorrelationId()).To(BeNumerically("==", 42))
		})

		It("can return the size needed to encode the frame", func() {
			c := NewConsumerUpdateQuery(42, 1)
			c.SetCorrelationId(123)

			buff := &bytes.Buffer{}
			wr := bufio.NewWriter(buff)
			Expect(c.Write(wr)).To(BeNumerically("==", c.SizeNeeded()-streamProtocolCorrelationIdSizeBytes))
			Expect(wr.Flush()).To(Succeed())

			expectedByteSequence := []byte{
				0x00, 0x00, 0x00, 0x7B, // correlationId
				0x2A, // subscriptionId
				0x01, // active
			}
			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})

	Context("Response", func() {
		//ConsumerUpdateResponse => Key Version CorrelationId ResponseCode OffsetSpecification
		//Key => uint16 // 0x801a
		//Version => uint16
		//CorrelationId => uint32
		//ResponseCode => uint16
		//OffsetSpecification => OffsetType Offset
		//OffsetType => uint16 // 0 (none), 1 (first), 2 (last), 3 (next), 4 (offset), 5 (timestamp)
		//Offset => uint64 (for offset) | int64 (for timestamp)
		It("has the expected fields", func() {
			resp := NewConsumerUpdateResponse(123, 1, 3, 1)
			Expect(resp.CorrelationId()).To(BeNumerically("==", 123))
			Expect(resp.ResponseCode()).To(BeNumerically("==", 1))
			Expect(resp.OffsetType()).To(BeNumerically("==", 3))
			Expect(resp.Offset()).To(BeNumerically("==", 1))
		})

		It("can encode itself into a binary sequence", func() {

			byteSequence := []byte{
				0x00, 0x00, 0x00, 0x7B, // uint32 correlation id
				0x00, 0x01, // uint 16 response code
				0x00, 0x03, // uint16  offset type
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // uint64 offset
			}

			resp := new(ConsumerUpdateResponse)
			Expect(resp.Read(bufio.NewReader(bytes.NewReader(byteSequence)))).To(Succeed())
			Expect(resp.correlationId).To(BeNumerically("==", 123))
			Expect(resp.responseCode).To(BeNumerically("==", 1))
			Expect(resp.offsetSpecification.offsetType).To(BeNumerically("==", 3))
			Expect(resp.offsetSpecification.offset).To(BeNumerically("==", 1))
		})
	})
})
