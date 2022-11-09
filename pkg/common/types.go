package common

import (
	"errors"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
)

var ResponseCodeToError = map[uint16]error{
	internal.ResponseCodeOK:                          nil, // this is a special case where there is not error
	internal.ResponseCodeStreamDoesNotExist:          errors.New("stream does not exist"),
	internal.ResponseCodeSubscriptionIdAlreadyExists: errors.New("subscription ID already exists"),
	internal.ResponseCodeSubscriptionIdDoesNotExist:  errors.New("subscription ID does not exist"),
	internal.ResponseCodeStreamAlreadyExists:         errors.New("stream already exists"),
	internal.ResponseCodeStreamNotAvailable:          errors.New("stream not available"),
	internal.ResponseCodeSASLMechanismNotSupported:   errors.New("SASL mechanism not supported"),
	internal.ResponseCodeAuthFailure:                 errors.New("authentication failure"),
	internal.ResponseCodeSASLError:                   errors.New("SASL error"),
	internal.ResponseCodeSASLChallenge:               errors.New("SASL challenge"),
	internal.ResponseCodeSASLAuthFailureLoopback:     errors.New("SASL authentication failure loopback"),
	internal.ResponseCodeVirtualHostAccessFailure:    errors.New("virtual host access failure"),
	internal.ResponseCodeUnknownFrame:                errors.New("unknown frame"),
	internal.ResponseCodeFrameTooLarge:               errors.New("frame too large"),
	internal.ResponseCodeInternalError:               errors.New("internal error"),
	internal.ResponseCodeAccessRefused:               errors.New("access refused"),
	internal.ResponseCodePreconditionFailed:          errors.New("precondition failed"),
	internal.ResponseCodePublisherDoesNotExist:       errors.New("publisher does not exist"),
	internal.ResponseCodeNoOffset:                    errors.New("no offset"),
}
