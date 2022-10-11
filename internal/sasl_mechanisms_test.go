package internal

import (
	"bufio"
	"bytes"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("SaslMechanisms", func() {
	Describe("SASL Handshake Request", func() {
		var (
			saslHandshakeReq *SaslHandshakeRequest
		)

		BeforeEach(func() {
			saslHandshakeReq = NewSaslHandshakeRequest()
		})

		It("returns the size required to encode the frame", func() {
			expectedSize := 2 + 2 + 4 // key ID + version + correlation ID
			Expect(saslHandshakeReq.SizeNeeded()).To(Equal(expectedSize))
		})

		It("binary encodes the struct into a binary sequence", func() {
			saslHandshakeReq.SetCorrelationId(123)
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)

			bytesWritten, err := saslHandshakeReq.Write(wr)
			Expect(err).ToNot(HaveOccurred())
			Expect(wr.Flush()).To(Succeed())

			Expect(bytesWritten).To(BeNumerically("==", 4))

			expectedByteSequence := []byte{0x00, 0x00, 0x00, 123}
			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})

		It("has the expected key/command ID", func() {
			Expect(saslHandshakeReq.Key()).To(BeNumerically("==", 18))
		})
	})

	Describe("SASL Handshake Response", func() {
		It("decodes a binary sequence into a struct", func() {
			someResponse := NewSaslHandshakeResponse()

			binarySasl := []byte{
				0x00, 0x00, 0x00, 0xFF, // correlation id
				0x00, 0x0a, // response code
				0x00, 0x00, 0x00, 0x02, // list size
				0x00, 0x03, // string len
				byte('f'), byte('o'), byte('o'),
				0x00, 0xF0, // string len
			}
			s := "VeryLongStringBecauseSometimesTheyBreakThingsAndMakePeopleSadWhenThingsDontWork!" +
				"VeryLongStringBecauseSometimesTheyBreakThingsAndMakePeopleSadWhenThingsDontWork!" +
				"VeryLongStringBecauseSometimesTheyBreakThingsAndMakePeopleSadWhenThingsDontWork!"
			binarySasl = append(binarySasl, []byte(s)...)
			buff := bytes.NewReader(binarySasl)
			rd := bufio.NewReader(buff)

			Expect(someResponse.Read(rd)).To(Succeed())

			Expect(someResponse.CorrelationId()).To(BeNumerically("==", 255))
			Expect(someResponse.responseCode).To(BeNumerically("==", 10))
			Expect(someResponse.Mechanisms).To(HaveLen(2))
			Expect(someResponse.Mechanisms).To(ConsistOf("foo", s))
		})
	})

	Describe("SASL Authenticate Request", func() {
		var (
			saslReq *SaslAuthenticateRequest
		)

		BeforeEach(func() {
			saslReq = NewSaslAuthenticateRequest("very-secure")
		})

		It("has the expected attributes", func() {
			saslReq := saslReq
			saslReq.SetCorrelationId(255)

			Expect(saslReq.CorrelationId()).To(BeNumerically("==", 255))
			Expect(saslReq.Key()).To(BeNumerically("==", 0x0013))
			Expect(saslReq.SizeNeeded()).To(BeNumerically("==", 4+ // correlation id
				2+11+ // string_len + string
				2+ // challenge response len
				2+2, // command id + version
			))
		})

		It("encodes the challenge response", func() {
			saslReq := saslReq
			Expect(saslReq.SetChallengeResponse(newSaslPlainMechanism("foo", "bar"))).To(Succeed())

			expectedByteSequence := []byte("\u0000foo\u0000bar")
			Expect(saslReq.saslOpaqueData).To(Equal(expectedByteSequence))
		})

		It("binary encodes the struct", func() {
			saslReq := saslReq
			saslReq.SetCorrelationId(255)
			Expect(saslReq.SetChallengeResponse(newSaslPlainMechanism("user", "password"))).To(Succeed())
			buff := new(bytes.Buffer)
			wr := bufio.NewWriter(buff)

			bytesWritten, err := saslReq.Write(wr)
			Expect(err).ToNot(HaveOccurred())
			Expect(wr.Flush()).To(Succeed())

			expectedBytesWritten := streamProtocolCorrelationIdSizeBytes +
				streamProtocolStringLenSizeBytes +
				+len("very-secure") + // "very-secure"
				streamProtocolSaslChallengeResponseLenBytes +
				len([]byte("\u0000user\u0000password"))
			Expect(bytesWritten).To(BeNumerically("==", expectedBytesWritten))

			expectedByteSequence := []byte{
				0x00, 0x00, 0x00, 0xff, // correlation id
				0x00, 0x0b, // mechanism_len
			}
			expectedByteSequence = append(expectedByteSequence, []byte("very-secure")...)
			expectedByteSequence = append(expectedByteSequence, 0x00, 0x0e) // challenge response len
			expectedByteSequence = append(expectedByteSequence, []byte("\u0000user\u0000password")...)

			Expect(buff.Bytes()).To(Equal(expectedByteSequence))
		})
	})

	Describe("SASL Authenticate Response", func() {

		When("there is no challenge data", func() {
			It("decodes a binary response correctly", func() {
				binaryResponse := []byte{
					0x00, 0x00, 0x00, 0x0F, // correlation id
					0x00, 0x0b, // response code
				}

				saslResp := new(SaslAuthenticateResponse)
				Expect(saslResp.Read(bufio.NewReader(bytes.NewReader(binaryResponse)))).To(Succeed())

				Expect(saslResp.CorrelationId()).To(BeNumerically("==", 15))
				Expect(saslResp.responseCode).To(BeNumerically("==", 11))
				Expect(saslResp.saslOpaqueData).To(BeNil())
			})
		})

		When("there is challenge data", func() {
			It("decodes a binary response correctly", func() {
				binaryResponse := []byte{
					0x00, 0x00, 0x00, 0x0F, // correlation id
					0x00, 0x0a, // response code
					0x00, 0x03, // sasl challenge len
				}
				binaryResponse = append(binaryResponse, []byte("foo")...)

				saslResp := new(SaslAuthenticateResponse)
				Expect(saslResp.Read(bufio.NewReader(bytes.NewReader(binaryResponse)))).To(Succeed())

				Expect(saslResp.CorrelationId()).To(BeNumerically("==", 15))
				Expect(saslResp.responseCode).To(BeNumerically("==", 10))

				expectedChallenge := []byte("foo")
				Expect(saslResp.saslOpaqueData).To(Equal(expectedChallenge))
			})
		})
	})
})
