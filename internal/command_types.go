package internal

import (
	"bufio"
)

type CommandRead interface {
	Read(reader *bufio.Reader) error
	CorrelationId() uint32
}

type CommandWrite interface {
	Write(writer *bufio.Writer) (int, error)
	Key() uint16
	// SizeNeeded must return the size required to encode this CommandWrite
	// plus the size of the Header. The size of the Header is always 4 bytes
	SizeNeeded() int
	SetCorrelationId(id uint32)
	CorrelationId() uint32
	Version() int16
}

// command IDs
const (
	CommandPeerProperties = 0x0011 // 17
	CommandSaslMechanisms = 0x0012 // 18
)

// Stream protocol field sizes
const (
	streamProtocolKeySizeBytes           = 2
	streamProtocolVersionSizeBytes       = 2
	streamProtocolCorrelationIdSizeBytes = 4
)

const (
	Version1 = int16(1)
)

// Stream protocol response codes
const (
	ResponseCodeOK                          uint16 = 0x01
	ResponseCodeStreamDoesNotExist          uint16 = 0x02
	ResponseCodeSubscriptionIdAlreadyExists uint16 = 0x03
	ResponseCodeSubscriptionIdDoesNotExist  uint16 = 0x04
	ResponseCodeStreamAlreadyExists                = 0x05
	ResponseCodeStreamNotAvailable                 = 0x06
	ResponseCodeSASLMechanismNotSupported   uint16 = 0x07
	ResponseCodeAuthFailure                 uint16 = 0x08
	ResponseCodeSASLError                   uint16 = 0x09
	ResponseCodeSASLChallenge               uint16 = 0x0a
	ResponseCodeSASLAuthFailureLoopback     uint16 = 0x0b
	ResponseCodeVirtualHostAccessFailure    uint16 = 0x0c
	ResponseCodeUnknownFrame                uint16 = 0x0d
	ResponseCodeFrameTooLarge               uint16 = 0x0e
	ResponseCodeInternalError               uint16 = 0x0f
	ResponseCodeAccessRefused               uint16 = 0x10
	ResponseCodePreconditionFailed          uint16 = 0x11
	ResponseCodePublisherDoesNotExist       uint16 = 0x12
	ResponseCodeNoOffset                    uint16 = 0x13
)
