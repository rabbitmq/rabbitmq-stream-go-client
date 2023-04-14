//go:build rabbitmq.stream.test

package raw

import (
	"context"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/internal"
)

// StartFrameListener starts reading the Connection socket. It receives frames
// from the connection and dispatches to the appropriate handler. This listener
// is started automatically by Connect(). Do NOT call this function directly.
// This function is a helper for running with fakes in tests.
func (tc *Client) StartFrameListener(ctx context.Context) {
	if ctx == nil {
		panic(errNilContext)
	}

	log := loggerFromCtxOrDiscard(ctx).WithGroup("frame-listener")
	log.Debug("starting frame listener")

	// it is ok to derive from ctx because this function
	// is used only in tests. It is not the same context
	// used in connection setup (because we use a fake rabbit)
	// In other words, this function is not affected by:
	// https://github.com/Gsantomaggio/rabbitmq-stream-go-client/issues/27
	ioLoopCtx, cancel := context.WithCancel(ctx)
	tc.ioLoopCancelFn = cancel

	err := tc.handleIncoming(ioLoopCtx)
	if err != nil {
		// FIXME: handle error, possibly shutdown or reconnect
		log.Error("error handling incoming frames", "error", err)
	}
}

// SetIsOpen sets the field isOpen. Useful during tests to simulate that the
// connection is open.
func (tc *Client) SetIsOpen(open bool) {
	tc.isOpen = open
}

// Request to expose Client.request for testing purposes
func (tc *Client) Request(ctx context.Context, write internal.CommandWrite) error {
	return tc.request(ctx, write)
}
