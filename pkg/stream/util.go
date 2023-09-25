package stream

import (
	"context"
	"errors"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"sync"
	"time"
)

const (
	maxAgeKey         = "x-max-age"
	maxLengthKey      = "x-max-length-bytes"
	maxSegmentSizeKey = "x-stream-max-segment-size-bytes"
)

func streamOptionsToRawStreamConfiguration(options CreateStreamOptions) raw.StreamConfiguration {
	c := make(raw.StreamConfiguration, 3)
	if options.MaxLength != 0 {
		c[maxLengthKey] = options.MaxLength.String()
	}

	if options.MaxSegmentSize != 0 {
		c[maxSegmentSizeKey] = options.MaxSegmentSize.String()
	}

	if options.MaxAge > 0 {
		c[maxAgeKey] = fmt.Sprintf("%.0fs", options.MaxAge.Seconds())
	}

	return c
}

func isNonRetryableError(err error) bool {
	return errors.Is(err, raw.ErrStreamAlreadyExists) ||
		errors.Is(err, raw.ErrSubscriptionIdAlreadyExists) ||
		errors.Is(err, raw.ErrSubscriptionIdDoesNotExist) ||
		errors.Is(err, raw.ErrStreamDoesNotExist) ||
		errors.Is(err, raw.ErrStreamNotAvailable) ||
		errors.Is(err, raw.ErrSASLMechanismNotSupported) ||
		errors.Is(err, raw.ErrAuthFailure) ||
		errors.Is(err, raw.ErrSASLError) ||
		errors.Is(err, raw.ErrSASLChallenge) ||
		errors.Is(err, raw.ErrSASLAuthFailureLoopback) ||
		errors.Is(err, raw.ErrVirtualHostAccessFailure) ||
		errors.Is(err, raw.ErrUnknownFrame) ||
		errors.Is(err, raw.ErrFrameTooLarge) ||
		errors.Is(err, raw.ErrInternalError) ||
		errors.Is(err, raw.ErrAccessRefused) ||
		errors.Is(err, raw.ErrPreconditionFailed) ||
		errors.Is(err, raw.ErrPublisherDoesNotExist) ||
		errors.Is(err, raw.ErrNoOffset) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, ErrUnsupportedOperation)
}

func maybeApplyDefaultTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); !ok {
		return context.WithTimeout(ctx, DefaultTimeout)
	}
	return ctx, nil
}

// Borrowed from experimental Go library
// https://pkg.go.dev/golang.org/x/exp/constraints#Integer
// Making a copy & paste to avoid depending on golang.org/x/exp
// FIXME probably need to include copyright/license from golang.org/x/exp
type integer interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

type autoIncrementingSequence[T integer] struct {
	sync.Mutex
	value T
}

func newAutoIncrementingSequence[T integer](startingValue T) *autoIncrementingSequence[T] {
	return &autoIncrementingSequence[T]{value: startingValue}
}

func (a *autoIncrementingSequence[T]) next() (next T) {
	a.Lock()
	defer a.Unlock()
	next = a.value
	a.value += 1
	return
}

type backoffDurationFunc func(int) time.Duration

var defaultBackOffPolicy backoffDurationFunc = func(i int) time.Duration {
	return time.Second * time.Duration(i<<1)
}

func validateStringParameter(p string) bool {
	if len(p) == 0 || p == " " {
		return false
	}

	return true
}
