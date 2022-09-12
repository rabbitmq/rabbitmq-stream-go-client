package internal

import (
	"github.com/hashicorp/go-multierror"
)

// MultiError is a type that can hold multiple errors.
type MultiError struct {
	result *multierror.Error
}

func NewMultiError() *MultiError {
	return &MultiError{result: &multierror.Error{}}
}

func (e *MultiError) ErrorOrNil() error {
	return e.result.ErrorOrNil()
}

func (e *MultiError) MaybeAddError(err error, message ...string) {
	MaybeLogError(err, message...)
	if err != nil {
		e.result = multierror.Append(e.result, err)
	}
}
