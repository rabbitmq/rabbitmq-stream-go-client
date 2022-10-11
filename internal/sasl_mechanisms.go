package internal

import (
	"bufio"
	"bytes"
	"encoding"
	"fmt"
)

type SaslHandshakeRequest struct {
	correlationId uint32
}

func (s *SaslHandshakeRequest) Version() int16 {
	return Version1
}

func NewSaslHandshakeRequest() *SaslHandshakeRequest {
	return &SaslHandshakeRequest{}
}

func (s *SaslHandshakeRequest) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, s.CorrelationId())
}

func (s *SaslHandshakeRequest) SizeNeeded() int {
	return streamProtocolKeySizeBytes +
		streamProtocolVersionSizeBytes +
		streamProtocolCorrelationIdSizeBytes
}

func (s *SaslHandshakeRequest) SetCorrelationId(id uint32) {
	s.correlationId = id
}

func (s *SaslHandshakeRequest) CorrelationId() uint32 {
	return s.correlationId
}

func (s *SaslHandshakeRequest) Key() uint16 {
	return CommandSaslHandshake
}

type SaslHandshakeResponse struct {
	correlationId uint32
	responseCode  uint16
	Mechanisms    []string
}

func NewSaslHandshakeResponse() *SaslHandshakeResponse {
	return &SaslHandshakeResponse{}
}

func (s *SaslHandshakeResponse) Read(reader *bufio.Reader) error {
	var mechanismsCount uint32
	err := readMany(reader, &s.correlationId, &s.responseCode, &mechanismsCount)
	if err != nil {
		return err
	}
	s.responseCode = ExtractResponseCode(s.responseCode)
	for i := uint32(0); i < mechanismsCount; i++ {
		s.Mechanisms = append(s.Mechanisms, readString(reader))
	}
	return nil
}

func (s *SaslHandshakeResponse) CorrelationId() uint32 {
	return s.correlationId
}

type SaslAuthenticateRequest struct {
	correlationId  uint32
	mechanism      string
	saslOpaqueData []byte
}

func NewSaslAuthenticateRequest(mechanism string) *SaslAuthenticateRequest {
	return &SaslAuthenticateRequest{mechanism: mechanism}
}

func (s *SaslAuthenticateRequest) SetChallengeResponse(challengeEncode encoding.BinaryMarshaler) error {
	encodedChallenge, err := challengeEncode.MarshalBinary()
	if err != nil {
		return err
	}
	s.saslOpaqueData = encodedChallenge
	return nil
}

func (s *SaslAuthenticateRequest) Write(writer *bufio.Writer) (int, error) {
	n, err := writeMany(
		writer,
		s.correlationId,
		s.mechanism,
		uint16(len(s.saslOpaqueData)),
		s.saslOpaqueData,
	)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (s *SaslAuthenticateRequest) Key() uint16 {
	return CommandSaslAuthenticate
}

func (s *SaslAuthenticateRequest) SizeNeeded() int {
	return streamProtocolKeySizeBytes + streamProtocolVersionSizeBytes +
		streamProtocolCorrelationIdSizeBytes +
		streamProtocolStringLenSizeBytes + len(s.mechanism) +
		streamProtocolStringLenSizeBytes + len(s.saslOpaqueData)
}

func (s *SaslAuthenticateRequest) SetCorrelationId(id uint32) {
	s.correlationId = id
}

func (s *SaslAuthenticateRequest) CorrelationId() uint32 {
	return s.correlationId
}

func (s *SaslAuthenticateRequest) Version() int16 {
	return Version1
}

type saslPlainMechanism struct {
	username, password string
}

func newSaslPlainMechanism(username string, password string) *saslPlainMechanism {
	return &saslPlainMechanism{username: username, password: password}
}

func (s saslPlainMechanism) MarshalBinary() (data []byte, err error) {
	buff := new(bytes.Buffer)
	bytesWritten := 0
	n, err := buff.WriteRune('\u0000')
	if err != nil {
		return nil, err
	}
	bytesWritten += n

	n, err = buff.WriteString(s.username)
	if err != nil {
		return nil, err
	}
	bytesWritten += n

	n, err = buff.WriteRune('\u0000')
	if err != nil {
		return nil, err
	}
	bytesWritten += n

	n, err = buff.WriteString(s.password)
	if err != nil {
		return nil, err
	}
	bytesWritten += n

	data = make([]byte, bytesWritten)
	bytesCopied := copy(data, buff.Bytes())
	if bytesWritten != bytesCopied {
		return nil, fmt.Errorf(
			"MarshalBinary did not write as many bytes as expected: copy %d want %d",
			bytesCopied,
			bytesWritten,
		)
	}
	return data, nil
}

type SaslAuthenticateResponse struct {
	correlationId  uint32
	responseCode   uint16
	saslOpaqueData []byte
}

func (s *SaslAuthenticateResponse) Read(reader *bufio.Reader) error {
	err := readMany(reader, &s.correlationId, &s.responseCode)
	if err != nil {
		return err
	}

	if s.responseCode == ResponseCodeSASLChallenge {
		challengeResponse, err := readByteSlice(reader)
		if err != nil {
			return err
		}
		s.saslOpaqueData = challengeResponse
	}
	return nil
}

func (s *SaslAuthenticateResponse) CorrelationId() uint32 {
	return s.correlationId
}
