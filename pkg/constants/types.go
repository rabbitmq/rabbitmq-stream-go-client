package constants

import (
	"errors"
)

var ResponseCodeToError = map[uint16]error{
	ResponseCodeOK:                          nil, // this is a special case where there is not error
	ResponseCodeStreamDoesNotExist:          errors.New("stream does not exist"),
	ResponseCodeSubscriptionIdAlreadyExists: errors.New("subscription ID already exists"),
	ResponseCodeSubscriptionIdDoesNotExist:  errors.New("subscription ID does not exist"),
	ResponseCodeStreamAlreadyExists:         errors.New("stream already exists"),
	ResponseCodeStreamNotAvailable:          errors.New("stream not available"),
	ResponseCodeSASLMechanismNotSupported:   errors.New("SASL mechanism not supported"),
	ResponseCodeAuthFailure:                 errors.New("authentication failure"),
	ResponseCodeSASLError:                   errors.New("SASL error"),
	ResponseCodeSASLChallenge:               errors.New("SASL challenge"),
	ResponseCodeSASLAuthFailureLoopback:     errors.New("SASL authentication failure loopback"),
	ResponseCodeVirtualHostAccessFailure:    errors.New("virtual host access failure"),
	ResponseCodeUnknownFrame:                errors.New("unknown frame"),
	ResponseCodeFrameTooLarge:               errors.New("frame too large"),
	ResponseCodeInternalError:               errors.New("internal error"),
	ResponseCodeAccessRefused:               errors.New("access refused"),
	ResponseCodePreconditionFailed:          errors.New("precondition failed"),
	ResponseCodePublisherDoesNotExist:       errors.New("publisher does not exist"),
	ResponseCodeNoOffset:                    errors.New("no offset found"),
}

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

type StreamConfiguration = map[string]string
type SubscribeProperties = map[string]string

const (
	OffsetTypeFirst     uint16 = 0x01
	OffsetTypeLast      uint16 = 0x02
	OffsetTypeNext      uint16 = 0x03
	OffsetTypeOffset    uint16 = 0x04
	OffsetTypeTimeStamp uint16 = 0x05
)

// CompressionType is the type of compression used for subBatchEntry
// See common/types.go Compresser interface
// See client::SendSubEntryBatch
const (
	CompressionNone uint8 = 0x00
	CompressionGzip uint8 = 0x01
)

// Connections states
const (
	ConnectionClosed  = 0x01
	ConnectionOpen    = 0x02
	ConnectionClosing = 0x03
)
