package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Route", func() {
	//RouteQuery => Key Version CorrelationId RoutingKey SuperStream
	//Key => uint16 // 0x0018
	//Version => uint16
	//CorrelationId => uint32
	//RoutingKey => string
	//SuperStream => string

	Describe("RouteQuery", func() {
		var routingKey string
		var superStream string
		var correlationId uint32

		BeforeEach(func() {
			routingKey = "key"
			superStream = "sStream"
			correlationId = 42
		})
		It("returns the size needed to encode the frame", func() {
			routeQuery := NewRouteQuery(routingKey, superStream)
			routeQuery.SetCorrelationId(correlationId)

			expectedSize := 2 + 2 + // key ID + version
				4 + // correlationID
				2 + 3 + // uint16 for the routing key string + uint32 string length
				2 + 7 // uint16 for the super stream string + uint32 string length

			Expect(routeQuery.SizeNeeded()).To(Equal(expectedSize))
		})

		It("has the required fields", func() {
			routeQuery := NewRouteQuery(routingKey, superStream)
			Expect(routeQuery.Key()).To(BeNumerically("==", 0x0018))
			Expect(routeQuery.Version()).To(BeNumerically("==", 1))
			Expect(routeQuery.RoutingKey()).To(Equal("key"))
			Expect(routeQuery.SuperStream()).To(Equal("sStream"))
		})

		It("can encode itself into a binary sequence", func() {
			routeQuery := NewRouteQuery(routingKey, superStream)
			routeQuery.SetCorrelationId(correlationId)
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)
			bytesWritten, err := routeQuery.Write(wr)

			Expect(err).NotTo(HaveOccurred())
			Expect(wr.Flush()).To(Succeed())
			Expect(bytesWritten).To(BeNumerically(
				"==", routeQuery.SizeNeeded()-streamProtocolHeaderSizeBytes))

			expectedByteSequence := []byte{
				0x00, 0x00, 0x00, 0x2A, // correlationId
				0x00, 0x03, byte('k'), byte('e'), byte('y'), // routing key len + reference string
				0x00, 0x07, // superStream len
				byte('s'), byte('S'), byte('t'), byte('r'), byte('e'), byte('a'), byte('m'), // superStream string
			}

			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})
})
