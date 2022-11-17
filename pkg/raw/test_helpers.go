//go:build rabbitmq.stream.test

package raw

import (
	"context"
	"github.com/go-logr/logr"
)

// StartFrameListener starts reading the Connection socket. It receives frames
// from the connection and dispatches to the appropriate handler. This listener
// is started automatically by Connect(). Do NOT call this function directly.
// This function is a helper for running with fakes in tests.
func (tc *Client) StartFrameListener(ctx context.Context) {
	if ctx == nil {
		panic(errNilContext)
	}
	log := logr.FromContextOrDiscard(ctx).WithName("frame-listener")
	log.V(debugLevel).Info("starting frame listener")
	err := tc.handleIncoming(ctx)
	if err != nil {
		// FIXME: handle error, possibly shutdown or reconnect
		log.Error(err, "error handling incoming frames")
	}
}
