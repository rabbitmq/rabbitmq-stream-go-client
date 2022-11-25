package internal

import (
	"bufio"
)

type CommandRead interface {
	Read(reader *bufio.Reader) error
	CorrelationId() uint32
	ResponseCode() uint16
}

// CommandWrite is the interface that wraps the Write method.
// The interface is implemented by all commands that are sent to the server.
// and that have no responses. Fire and forget style
// Command like: Publish and Store Offset.
type CommandWrite interface {
	Write(writer *bufio.Writer) (int, error)
	Key() uint16
	// SizeNeeded must return the size required to encode this CommandWrite
	// plus the size of the Header. The size of the Header is always 4 bytes
	SizeNeeded() int
	Version() int16
}

// SyncCommandWrite is the interface that wraps the Write method.
// The interface is implemented by all commands that are sent to the server.
// and that have responses in RPC style.
// Command like: Create Stream, Delete Stream, Declare Publisher, etc.
// SetCorrelationId CorrelationId is used to match the response with the request.
type SyncCommandWrite interface {
	Write(writer *bufio.Writer) (int, error)
	Key() uint16
	// SizeNeeded must return the size required to encode this SyncCommandWrite
	// plus the size of the Header. The size of the Header is always 4 bytes
	SizeNeeded() int
	SetCorrelationId(id uint32)
	CorrelationId() uint32
	Version() int16
}

// WriteCommand sends the Commands to the server.
// The commands are sent in the following order:
// 1. Header
// 2. Command
// 3. Flush
// The flush is required to make sure that the commands are sent to the server.
// WriteCommand doesn't care about the response.
func WriteCommand[T CommandWrite](request T, writer *bufio.Writer) error {
	hWritten, err := NewHeaderRequest(request).Write(writer)
	if err != nil {
		return err
	}
	bWritten, err := request.Write(writer)
	if err != nil {
		return err
	}
	if (bWritten + hWritten) != (request.SizeNeeded() + 4) {
		panic("Write Command: Not all bytes written")
	}
	return writer.Flush()
}

// command IDs
const (
	CommandDeclarePublisher uint16 = 0x0001 // 1
	CommandCreate           uint16 = 0x000d // 13
	CommandDelete           uint16 = 0x000e // 14
	CommandPeerProperties   uint16 = 0x0011 // 17
	CommandSaslHandshake    uint16 = 0x0012 // 18
	CommandSaslAuthenticate uint16 = 0x0013 // 19
	CommandTune             uint16 = 0x0014 // 20
	CommandOpen             uint16 = 0x0015 // 21
	CommandClose            uint16 = 0x0016 // 22
)

// Stream protocol field sizes
const (
	streamProtocolHeader = streamProtocolKeySizeBytes +
		streamProtocolVersionSizeBytes +
		streamProtocolCorrelationIdSizeBytes
	streamProtocolKeySizeBytes                  = 2
	streamProtocolKeySizeUint8                  = 1
	streamProtocolVersionSizeBytes              = 2
	streamProtocolCorrelationIdSizeBytes        = 4
	streamProtocolStringLenSizeBytes            = 2
	streamProtocolMapLenBytes                   = 4
	streamProtocolMapKeyLengthBytes             = 2
	streamProtocolMapValueLengthBytes           = 2
	streamProtocolSaslChallengeResponseLenBytes = 4
	streamProtocolMaxFrameSizeBytes             = 4
	streamProtocolHeartbeatPeriodBytes          = 4
	streamProtocolClosingCodeSizeBytes          = 2
	streamProtocolResponseCodeSizeBytes         = 2
	streamProtocolHeaderSizeBytes               = 4
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
	ResponseCodeStreamAlreadyExists         uint16 = 0x05
	ResponseCodeStreamNotAvailable          uint16 = 0x06
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
