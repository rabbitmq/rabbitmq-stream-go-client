package internal

import (
	"bufio"
	"errors"
)

//go:generate mockgen -source command_types.go -destination ../pkg/raw/mock_internal_interfaces_test.go -package raw_test

// CommandRead is the interface that wraps the Read method.
// Read reads the command from the reader.
// nto related to any correlation ID. for example publish confirm
type CommandRead interface {
	Read(reader *bufio.Reader) error
}

// SyncCommandRead reads the response from the stream.
// It reads the header and then the response.
// the Sync part is related to the correlation ID.
// So the caller waits for the response based on correlation
type SyncCommandRead interface {
	CommandRead
	CorrelationId() uint32
	ResponseCode() uint16
}

// CommandWrite is the interface that wraps the Write method.
// The interface is implemented by all commands that are sent to the server.
// and that have no responses. Fire and forget style
// Command like: PublishRequest and Store Offset.
type CommandWrite interface {
	Write(writer *bufio.Writer) (int, error)
	Key() uint16
	// SizeNeeded must return the size required to encode this CommandWrite
	// plus the size of the Header. The size of the Header is always 4 bytes
	SizeNeeded() int
	Version() int16
}

// SyncCommandWrite is the interface that wraps the WriteTo method.
// The interface is implemented by all commands that are sent to the server.
// and that have responses in RPC style.
// Command like: Create Stream, Delete Stream, Declare Publisher, etc.
// SetCorrelationId CorrelationId is used to match the response with the request.
type SyncCommandWrite interface {
	CommandWrite // Embedding the CommandWrite interface
	SetCorrelationId(id uint32)
	CorrelationId() uint32
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
		panic("WriteTo Command: Not all bytes written")
	}
	return writer.Flush()
}

type commandInformer interface {
	Key() uint16
	MinVersion() int16
	MaxVersion() int16
}

// command IDs
const (
	CommandDeclarePublisher        uint16 = 0x0001 // 1
	CommandPublish                 uint16 = 0x0002 // 2
	CommandPublishConfirm          uint16 = 0x0003 // 3
	CommandDeletePublisher         uint16 = 0x0006 // 6
	CommandSubscribe               uint16 = 0x0007 // 7
	CommandDeliver                 uint16 = 0x0008 // 8
	CommandCredit                  uint16 = 0x0009 // 9
	CommandStoreOffset             uint16 = 0x000a // 10
	CommandQueryOffset             uint16 = 0x000b // 11
	CommandCreate                  uint16 = 0x000d // 13
	CommandDelete                  uint16 = 0x000e // 14
	CommandMetadata                uint16 = 0x000f // 15
	CommandPeerProperties          uint16 = 0x0011 // 17
	CommandSaslHandshake           uint16 = 0x0012 // 18
	CommandSaslAuthenticate        uint16 = 0x0013 // 19
	CommandTune                    uint16 = 0x0014 // 20
	CommandOpen                    uint16 = 0x0015 // 21
	CommandClose                   uint16 = 0x0016 // 22
	CommandExchangeCommandVersions uint16 = 0x001b // 27
	CommandStreamStats             uint16 = 0x001c // 28
)

const (
	CommandDeclarePublisherResponse        uint16 = 0x8001
	CommandDeletePublisherResponse         uint16 = 0x8006
	CommandSubscribeResponse               uint16 = 0x8007
	CommandCreditResponse                  uint16 = 0x8009
	CommandQueryOffsetResponse             uint16 = 0x800b
	CommandCreateResponse                  uint16 = 0x800d
	CommandDeleteResponse                  uint16 = 0x800e
	CommandMetadataResponse                uint16 = 0x800f
	CommandPeerPropertiesResponse          uint16 = 0x8011
	CommandSaslHandshakeResponse           uint16 = 0x8012
	CommandSaslAuthenticateResponse        uint16 = 0x8013
	CommandTuneResponse                    uint16 = 0x8014
	CommandOpenResponse                    uint16 = 0x8015
	CommandCloseResponse                   uint16 = 0x8016
	CommandExchangeCommandVersionsResponse uint16 = 0x801b
)

// Stream protocol field sizes
const (
	streamProtocolHeader = streamProtocolKeySizeBytes +
		streamProtocolVersionSizeBytes +
		streamProtocolCorrelationIdSizeBytes
	streamProtocolKeySizeBytes                  = 2
	streamProtocolKeySizeUint8                  = 1
	streamProtocolKeySizeUint16                 = 2
	streamProtocolKeySizeUint32                 = 4
	streamProtocolKeySizeUint64                 = 8
	streamProtocolVersionSizeBytes              = 2
	streamProtocolCorrelationIdSizeBytes        = 4
	streamProtocolStringLenSizeBytes            = 2
	streamProtocolSliceLenBytes                 = 4
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
	Version1 int16 = iota + 1
	Version2
)

var errWriteShort = errors.New("wrote less bytes than expected")
