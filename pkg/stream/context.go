package stream

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"
)

// ownedConnection interrupts writes without acquiring the socket mutex held by
// the blocked writer, and detaches its cancellation callback when it closes.
type ownedConnection struct {
	net.Conn
	stop func() bool
}

func (c *ownedConnection) Close() error { c.stop(); return c.Conn.Close() }
func (p *TCPParameters) connectionContextError() error {
	if p.connectionContext != nil {
		return p.connectionContext.Err()
	}
	return nil
}
func (c *Client) connectionContext() context.Context {
	if c.tcpParameters.connectionContext != nil {
		return c.tcpParameters.connectionContext
	}
	return context.Background()
}
func (c *Client) connectionError(err error) error {
	if contextErr := c.connectionContext().Err(); contextErr != nil {
		return errors.Join(err, contextErr)
	}
	return err
}
func (c *Client) waitDelay(delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-c.connectionContext().Done():
		return c.connectionContext().Err()
	case <-timer.C:
		return nil
	}
}
func (c *Client) waitData(response *Response) (value any, err error) {
	defer func() {
		if err != nil {
			c.coordinator.discardResponse(response)
		}
	}()
	// Preserve existing response-wait behavior for NewEnvironment callers.
	if c.tcpParameters.connectionContext == nil {
		return <-response.data, nil
	}
	select {
	case value, ok := <-response.data:
		if !ok {
			return nil, c.connectionError(fmt.Errorf("response channel closed: %s", response.commandDescription))
		}
		return value, nil
	case <-c.connectionContext().Done():
		return nil, c.connectionContext().Err()
	}
}
func (c *Client) waitCode(response *Response) responseError {
	if c.tcpParameters.connectionContext == nil {
		return waitCodeWithTimeOut(response, c.socketCallTimeout)
	}
	timer := time.NewTimer(c.socketCallTimeout)
	defer timer.Stop()
	select {
	case code, ok := <-response.code:
		if !ok {
			return newResponseError(c.connectionError(fmt.Errorf("response channel closed: %s", response.commandDescription)), false)
		}
		if code.id != responseCodeOk {
			return newResponseError(lookErrorCode(code.id), false)
		}
		return newResponseError(nil, false)
	case <-c.connectionContext().Done():
		return newResponseError(c.connectionContext().Err(), false)
	case <-timer.C:
		return newResponseError(fmt.Errorf("timeout %d ms - waiting Code, operation: %s", c.socketCallTimeout.Milliseconds(), response.commandDescription), true)
	}
}
