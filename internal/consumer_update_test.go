package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ConsumerUpdate", func() {
	Context("Query", func() {
		//ConsumerUpdateQuery => Key Version CorrelationId SubscriptionId Active
		//Key => uint16 // 0x001a
		//Version => uint16
		//CorrelationId => uint32
		//SubscriptionId => uint8
		//Active => uint8 (boolean, 0 = false, 1 = true)

		It("has the expected fields", func() {
			q := NewConsumerUpdateQuery(42, 42, 1)

			Expect(q.CorrelationId()).To(BeNumerically("==", 42))
			Expect(q.SubscriptionId()).To(BeNumerically("==", 42))
			Expect(q.Active()).To(BeNumerically("==", 1))
		})

		It("can encode itself into a binary sequence", func() {
			q := NewConsumerUpdateQuery(42, 3, 1)

			byteSequence := []byte{
				0x00, 0x00, 0x00, 0x7B, // uint32 correlation id
				0x03, // uint 8 subscriptioId
				0x01, // uint 8  active
			}

			Expect(q.Read(bufio.NewReader(bytes.NewReader(byteSequence)))).To(Succeed())
			Expect(q.correlationId).To(BeNumerically("==", 123))
			Expect(q.subscriptionId).To(BeNumerically("==", 3))
			Expect(q.active).To(BeNumerically("==", 1))
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
			Expect(resp.Key()).To(BeNumerically("==", 0x801a))
			Expect(resp.CorrelationId()).To(BeNumerically("==", 123))
			Expect(resp.ResponseCode()).To(BeNumerically("==", 1))
			Expect(resp.OffsetType()).To(BeNumerically("==", 3))
			Expect(resp.Offset()).To(BeNumerically("==", 1))
		})

		It("can return the size needed to encode the frame", func() {
			c := NewConsumerUpdateResponse(123, 1, 16, 64)

			buff := &bytes.Buffer{}
			wr := bufio.NewWriter(buff)
			Expect(c.Write(wr)).To(BeNumerically("==", c.SizeNeeded()-streamProtocolHeaderSizeBytes))
			Expect(wr.Flush()).To(Succeed())

			expectedByteSequence := []byte{
				0x00, 0x00, 0x00, 0x7B, //  uint 32 correlationId
				0x00, 0x01, //  uint 16 responseCode
				0x00, 0x10, //  uint 16 offset Type
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // offset
			}
			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})
})
