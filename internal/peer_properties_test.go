package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("PeerProperties", func() {

	Context("Peer Properties request", func() {
		When("there is one peer property", func() {
			var pp = PeerPropertiesRequest{
				clientProperties: map[string]string{"foo": "bar"},
				correlationId:    789,
			}

			It("returns the correct size", func() {
				// PeerPropertiesRequest => Key Version PeerProperties
				//  Key => uint16 // 0x0011
				//  Version => uint16
				//  CorrelationId => uint32
				//  PeerProperties => uint32 + [PeerProperty]*
				//  PeerProperty => Key + Value
				//  Key => uint16 + string
				//  Value => uint16 + string
				expectedSize := 2 + // key
					2 + // version
					4 + // correlation id
					4 + // map size
					2 + 3 + // key
					2 + 3 // value
				Expect(pp.SizeNeeded()).To(BeNumerically("==", expectedSize))
			})

			It("binary encodes the struct correctly", func() {
				buff := new(bytes.Buffer)
				wr := bufio.NewWriter(buff)
				writtenBytes, err := pp.Write(wr)
				Expect(err).ToNot(HaveOccurred())
				Expect(wr.Flush()).To(Succeed())

				By("writing the expected number of bytes")
				expectedWrittenBytes := streamProtocolCorrelationIdSizeBytes +
					streamProtocolMapLenBytes +
					streamProtocolMapKeyLengthBytes +
					3 + // "foo"
					streamProtocolMapValueLengthBytes +
					3 // "bar"
				Expect(writtenBytes).To(Equal(expectedWrittenBytes))

				By("generating the expected byte sequence")
				expectedBytes := []byte{
					0x00, 0x00, 0x03, 0x15, // correlation id
					0x00, 0x00, 0x00, 0x01, // map size
					0x00, 0x03, // key length
					byte('f'), byte('o'), byte('o'),
					0x00, 0x03, // value length
					byte('b'), byte('a'), byte('r'),
				}
				Expect(buff.Bytes()).To(Equal(expectedBytes))
			})
		})

		When("there are no peer properties", func() {
			var pp = PeerPropertiesRequest{
				clientProperties: map[string]string{},
				correlationId:    456,
			}

			It("returns the correct size", func() {
				expectedSize := 2 + // key
					2 + // version
					4 + // correlation id
					4 // map size
				Expect(pp.SizeNeeded()).To(BeNumerically("==", expectedSize))
			})

			It("binary encodes the struct correctly", func() {
				buff := new(bytes.Buffer)
				wr := bufio.NewWriter(buff)
				writtenBytes, err := pp.Write(wr)
				Expect(err).ToNot(HaveOccurred())
				Expect(wr.Flush()).To(Succeed())

				By("writing the expected number of bytes")
				expectedWrittenBytes := streamProtocolCorrelationIdSizeBytes +
					streamProtocolMapLenBytes
				Expect(writtenBytes).To(Equal(expectedWrittenBytes))

				By("generating the expected byte sequence")
				expectedBytes := []byte{
					0x00, 0x00, 0x01, 0xc8, // correlation id
					0x00, 0x00, 0x00, 0x00, // map size
				}
				Expect(buff.Bytes()).To(Equal(expectedBytes))
			})
		})

		When("there are many properties", func() {
			var (
				pp = PeerPropertiesRequest{
					clientProperties: map[string]string{
						"one": "1",
						"two": "2",
						"bmw": "mercedes",
					},
					correlationId: 890,
				}
			)

			It("returns the correct size", func() {
				expectedSize := 2 + // key
					2 + // version
					4 + // correlation id
					4 + // map size
					2 + 3 + // key_length + key -- one
					2 + 1 + // value_length + value -- 1
					2 + 3 + // key_length + key -- two
					2 + 1 + // value_length + value -- 2
					2 + 3 + // key_length + key -- bmw
					2 + 8 // value_length + value -- mercedes
				Expect(pp.SizeNeeded()).To(BeNumerically("==", expectedSize))
			})

			// Adding FlakeAttempts because writeMany uses `range` to iterate
			// the map of peer properties. Range does not guarantee the iteration
			// order; therefore, the binary sequence may not always be what we expect,
			// although it will be correct, because we do not care about map item order
			// https://go.dev/ref/spec#RangeClause
			It("binary encodes the struct correctly", FlakeAttempts(7), func() {
				buff := new(bytes.Buffer)
				wr := bufio.NewWriter(buff)
				writtenBytes, err := pp.Write(wr)
				Expect(err).ToNot(HaveOccurred())
				Expect(wr.Flush()).To(Succeed())

				By("writing the expected number of bytes")
				expectedWrittenBytes := streamProtocolCorrelationIdSizeBytes +
					sizeNeededForMap(pp.clientProperties)
				Expect(writtenBytes).To(Equal(expectedWrittenBytes))

				By("generating the expected byte sequence")
				expectedBytes := []byte{
					0x00, 0x00, 0x03, 0x7a, // correlation id
					0x00, 0x00, 0x00, 0x03, // map size
					0x00, 0x03, // key length
					byte('o'), byte('n'), byte('e'),
					0x00, 0x01, // value length
					byte('1'),
					0x00, 0x03, // key length
					byte('t'), byte('w'), byte('o'),
					0x00, 0x01, // value length
					byte('2'),
					0x00, 0x03, // key length
					byte('b'), byte('m'), byte('w'),
					0x00, 0x08, // value length
					byte('m'), byte('e'), byte('r'), byte('c'), byte('e'), byte('d'),
					byte('e'), byte('s'), // "mercedes"
				}
				Expect(buff.Bytes()).To(Equal(expectedBytes))
			})
		})
	})

	Context("Peer Properties response", func() {
		When("the response has one peer property", func() {
			It("decodes a binary frame into a response struct", func() {
				var binaryPeerPropertyResponse = []byte{
					0x12, 0x34, 0x56, 0x78, // correlationID
					0x12, 0x34, // response code
					0x00, 0x00, 0x00, 0x01, // map size == 1
					0x00, 0x01, // key length
					0x61,       // "a"
					0x00, 0x01, // value length
					0x62, // "b"
				}
				rd := bytes.NewReader(binaryPeerPropertyResponse)
				pp := PeerPropertiesResponse{ServerProperties: make(map[string]string)}
				Expect(pp.Read(bufio.NewReader(rd))).To(Succeed())

				Expect(pp.correlationId).To(BeNumerically("==", 0x12345678))
				// Response code most significant bit is set to 0 during decoding
				Expect(pp.responseCode).To(BeNumerically("==", 0x1234))
				Expect(pp.ServerProperties).To(HaveKeyWithValue("a", "b"))
			})
		})

		When("the response has zero peer properties", func() {
			It("decodes a binary frame into a response struct", func() {
				var binaryPeerPropertyResponse = []byte{
					0x12, 0x34, 0x56, 0x78, // correlationID
					0x12, 0x34, // response code
					0x00, 0x00, 0x00, 0x00, // map size == 0
				}
				rd := bytes.NewReader(binaryPeerPropertyResponse)
				pp := PeerPropertiesResponse{
					correlationId:    0,
					responseCode:     0,
					ServerProperties: make(map[string]string),
				}
				Expect(pp.Read(bufio.NewReader(rd))).To(Succeed())

				Expect(pp.CorrelationId()).To(BeNumerically("==", 305419896))
				Expect(pp.responseCode).To(BeNumerically("==", 4660))
				Expect(pp.ServerProperties).To(HaveLen(0))
			})

		})

		When("the response has many peer properties", func() {
			It("decodes a binary frame into a response struct", func() {
				var binaryPeerPropertyResponse = []byte{
					0x12, 0x34, 0x56, 0x78, // correlationID
					0x12, 0x34, // response code
					0x00, 0x00, 0x00, 0x03, // map size == 3
					0x00, 0x01, // key length
					byte('a'),  // "a"
					0x00, 0x01, // value length
					byte('b'),  // "b"
					0x00, 0x03, // key2 length
					byte('f'), byte('o'), byte('o'), // "foo"
					0x00, 0x03, // value2 length
					byte('b'), byte('a'), byte('r'), // "bar"
					0x00, 0x02, // key3 length
					byte('h'), byte('i'), // "hi"
					0x00, 0x03, // value3 length
					byte('b'), byte('y'), byte('e'), // "bye"
				}
				rd := bytes.NewReader(binaryPeerPropertyResponse)
				pp := PeerPropertiesResponse{
					correlationId:    0,
					responseCode:     0,
					ServerProperties: make(map[string]string),
				}
				Expect(pp.Read(bufio.NewReader(rd))).To(Succeed())

				Expect(pp.CorrelationId()).To(BeNumerically("==", 305419896))
				Expect(pp.responseCode).To(BeNumerically("==", 4660))
				Expect(pp.ServerProperties).To(HaveLen(3))
				Expect(pp.ServerProperties).To(HaveKeyWithValue("a", "b"))
				Expect(pp.ServerProperties).To(HaveKeyWithValue("foo", "bar"))
				Expect(pp.ServerProperties).To(HaveKeyWithValue("hi", "bye"))
			})
		})
	})
})
