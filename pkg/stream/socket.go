package stream

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
)

type socket struct {
	connection net.Conn
	writer     *bufio.Writer
	mutex      *sync.Mutex
	closed     int32
	destructor *sync.Once
	// lifetimeErr reports whether an environment lifetime was canceled. It is nil
	// for NewEnvironment clients, which have no lifetime.
	lifetimeErr func() error
}

func (sck *socket) setOpen() {
	sck.mutex.Lock()
	defer sck.mutex.Unlock()
	sck.closed = 1
}

func (sck *socket) isOpen() bool {
	sck.mutex.Lock()
	defer sck.mutex.Unlock()
	return sck.closed == 1
}
func (sck *socket) shutdown(_ error) {
	if !sck.isOpen() {
		return
	}
	sck.mutex.Lock()
	sck.closed = 0
	sck.mutex.Unlock()

	sck.destructor.Do(func() {
		sck.mutex.Lock()
		defer sck.mutex.Unlock()
		err := sck.connection.Close()
		if err == nil {
			return
		}
		// Cancellation closes the connection first, so a second close reporting
		// ErrClosed is expected here. Every other case still warrants a warning,
		// because ErrClosed can also mean an unexpected disconnect.
		if errors.Is(err, net.ErrClosed) && sck.lifetimeErr != nil && sck.lifetimeErr() != nil {
			logs.LogDebug("socket already closed by lifetime cancellation: %s", err)
			return
		}
		logs.LogWarn("error during close socket: %s", err)
	})
}

func (sck *socket) writeAndFlush(buffer []byte) error {
	sck.mutex.Lock()
	defer sck.mutex.Unlock()
	_, err := sck.writer.Write(buffer)
	if err != nil {
		return err
	}
	err = sck.writer.Flush()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) handleWrite(buffer []byte, response *Response) responseError {
	return c.handleWriteWithResponse(buffer, response, true)
}

func (c *Client) handleWriteWithResponse(buffer []byte, response *Response, removeResponse bool) (resultCode responseError) {
	defer func() {
		if resultCode.Err != nil {
			c.coordinator.discardResponse(response)
		}
	}()

	// Fail fast: an over-sized frame makes the broker close the connection and we
	// would block until timeout. 0 = no limit.
	if fm := c.maxFrameSize(); fm > 0 && len(buffer) > fm {
		// The deferred discard removes the response; nothing was written, so no
		// reply can arrive for it.
		return newResponseError(
			fmt.Errorf("%w: frame size %d exceeds the maximum %d negotiated with the server, operation: %s",
				FrameTooLarge, len(buffer), fm, response.commandDescription), false)
	}

	if err := c.connectionContext().Err(); err != nil {
		return newResponseError(err, false)
	}
	result := c.socket.writeAndFlush(buffer)
	if result != nil {
		// A write interrupted by lifetime cancellation is expected, not a failure.
		if c.connectionContext().Err() == nil {
			logs.LogWarn("Error handleWrite %s", result)
		}
		return newResponseError(result, false)
	}

	resultCode = c.waitCode(response)
	if resultCode.Err != nil {
		// After a timeout or error code a frame reader may still hold this
		// response, so leave its channels open for the deferred discard.
		return resultCode
	}

	if removeResponse {
		_ = c.coordinator.RemoveResponseById(response.correlationid)
	}

	return resultCode
}
