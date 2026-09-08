package stream

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Hold the old reader's teardown after Close, as can happen when the next seed
// attempt runs before the reader observes a closed network connection.
type delayedBootstrapConn struct {
	net.Conn
	release  <-chan struct{}
	readDone chan struct{}
	closes   atomic.Int32
}

func (c *delayedBootstrapConn) Read([]byte) (int, error) {
	<-c.release
	close(c.readDone)
	return 0, io.EOF
}
func (c *delayedBootstrapConn) Write([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (c *delayedBootstrapConn) Close() error {
	c.closes.Add(1)
	return c.Conn.Close()
}

func TestBootstrapDoesNotReuseFailedHandshakeClient(t *testing.T) {
	for _, constructor := range []string{"legacy", "context"} {
		t.Run(constructor, func(t *testing.T) {
			local, peer := net.Pipe()
			release := make(chan struct{})
			connection := &delayedBootstrapConn{Conn: local, release: release, readDone: make(chan struct{})}
			t.Cleanup(func() {
				close(release)
				_ = local.Close()
				_ = peer.Close()
				select {
				case <-connection.readDone:
				case <-time.After(time.Second):
					t.Error("failed bootstrap reader did not finish")
				}
			})
			attempted := []string{}
			nextFailure := errors.New("second seed was actually dialed")
			options := NewEnvironmentOptions().SetUris([]string{
				"rabbitmq-stream://guest:guest@first:5552/",
				"rabbitmq-stream://guest:guest@second:5552/",
			})
			options.TCPParameters.dialContext = func(_ context.Context, _, address string) (net.Conn, error) {
				attempted = append(attempted, address)
				if address == "first:5552" {
					return connection, nil
				}
				return nil, nextFailure
			}
			var env *Environment
			var err error
			if constructor == "context" {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				env, err = NewEnvironmentWithContext(ctx, options)
			} else {
				env, err = NewEnvironment(options)
			}
			if env != nil {
				defer func() { _ = env.Close() }()
			}
			require.ErrorIs(t, err, nextFailure)
			assert.Equal(t, []string{"first:5552", "second:5552"}, attempted)
			assert.Positive(t, connection.closes.Load(), "failed handshake socket was not closed")
		})
	}
}
