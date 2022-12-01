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
	ResponseCodeNoOffset:                    errors.New("no offset"),
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
