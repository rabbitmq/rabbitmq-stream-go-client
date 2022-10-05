package internal

import (
	"bufio"
)

type SaslMechanismsRequest struct {
	key           uint16
	correlationId uint32
}

func (s *SaslMechanismsRequest) Version() int16 {
	return Version1
}

func NewSaslMechanismsRequest() *SaslMechanismsRequest {
	return &SaslMechanismsRequest{key: CommandSaslMechanisms}
}

func (s *SaslMechanismsRequest) Write(writer *bufio.Writer) (int, error) {
	return WriteMany(writer, s.CorrelationId())
}

func (s *SaslMechanismsRequest) SizeNeeded() int {
	return 8
}

func (s *SaslMechanismsRequest) SetCorrelationId(id uint32) {
	s.correlationId = id
}

func (s *SaslMechanismsRequest) CorrelationId() uint32 {
	return s.correlationId
}

func (s *SaslMechanismsRequest) Key() uint16 {
	return s.key
}

type SaslMechanismsResponse struct {
	correlationId uint32
	responseCode  uint16
	Mechanisms    []string
}

func NewSaslMechanismsResponse() *SaslMechanismsResponse {
	return &SaslMechanismsResponse{}
}

func (s *SaslMechanismsResponse) Read(reader *bufio.Reader) error {

	var mechanismsCount uint32
	err := ReadMany(reader, &s.correlationId, &s.responseCode, &mechanismsCount)
	s.responseCode = UShortExtractResponseCode(s.responseCode)
	MaybeLogError(err, "sasl mechanismsResponse read")
	for i := 0; i < int(mechanismsCount); i++ {
		s.Mechanisms = append(s.Mechanisms, ReadString(reader))
	}
	return nil
}

func (s *SaslMechanismsResponse) CorrelationId() uint32 {
	return s.correlationId
}
