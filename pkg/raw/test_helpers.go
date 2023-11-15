//go:build rabbitmq.stream.test

package raw

import (
	"context"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/internal"
)

// StartFrameListener starts reading the Connection socket. It receives frames
// from the connection and dispatches to the appropriate handler. This listener
// is started automatically by Connect(). Do NOT call this function directly.
// This function is a helper for running with fakes in tests.
func (tc *Client) StartFrameListener(ctx context.Context) {
	if ctx == nil {
		panic(errNilContext)
	}

	log := LoggerFromCtxOrDiscard(ctx).WithGroup("frame-listener")
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
		log.Error("error handling incoming frames", "error", err)
	}
}

// SetIsOpen sets the field connectionStatus. Useful during tests to simulate that the
// connection is open.
func (tc *Client) SetIsOpen(open bool) {
	if open {
		tc.connectionStatus = ConnectionOpen
	} else {
		tc.connectionStatus = ConnectionClosed
	}
}

// Request to expose Client.request for testing purposes
func (tc *Client) Request(ctx context.Context, write internal.CommandWrite) error {
	return tc.request(ctx, write)
}

func (tc *Client) ForceCloseConnectionSocket() {
	_ = tc.connection.Close()
}

// SetServerProperties sets the server properties given as key-value pairs.
// It is a mistake to provide an odd number of arguments, except 0 or 1.
// If given only 1 argument, it sets the argument as key with empty string value.
// Passing an odd number of arguments panics.
func (r *ClientConfiguration) SetServerProperties(keyValues ...string) {
	if r.RabbitmqAddr.ServerProperties == nil {
		r.RabbitmqAddr.ServerProperties = make(map[string]string)
	}

	if len(keyValues) == 0 {
		return
	}
	if len(keyValues) == 1 {
		r.RabbitmqAddr.ServerProperties[keyValues[0]] = ""
		return
	}
	for i := 0; i < len(keyValues); i += 2 {
		r.RabbitmqAddr.ServerProperties[keyValues[i]] = keyValues[i+1]
	}
}

func MetadataResponseForStream(name string) *MetadataResponse {
	return internal.NewMetadataResponse(
		123,         // correlation-id
		1234,        // port
		0,           // Broker reference
		1,           // response code
		0,           // leader reference
		"fakehost",  // host
		name,        // stream name
		[]uint16{0}, // replicas references
	)
}
