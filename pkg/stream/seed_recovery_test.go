package stream

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

// dialedSeeds records seed dial attempts. The dial runs on whichever goroutine
// bootstrap happens to use, so the spec reads the addresses through a lock.
type dialedSeeds struct {
	mutex     sync.Mutex
	addresses []string
}

func (d *dialedSeeds) record(address string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.addresses = append(d.addresses, address)
}

func (d *dialedSeeds) recorded() []string {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return append([]string(nil), d.addresses...)
}

var _ = Describe("Bootstrap seed recovery", func() {
	DescribeTable("dials the next seed with a fresh client after a failed handshake",
		func(withContext bool) {
			local, peer := net.Pipe()
			release := make(chan struct{})
			connection := &delayedBootstrapConn{Conn: local, release: release, readDone: make(chan struct{})}
			DeferCleanup(func() {
				close(release)
				_ = local.Close()
				_ = peer.Close()
				Eventually(connection.readDone, time.Second).Should(BeClosed(),
					"failed bootstrap reader did not finish")
			})

			attempted := &dialedSeeds{}
			nextFailure := errors.New("second seed was actually dialed")
			options := NewEnvironmentOptions().SetUris([]string{
				"rabbitmq-stream://guest:guest@first:5552/",
				"rabbitmq-stream://guest:guest@second:5552/",
			})
			options.TCPParameters.dialContext = func(_ context.Context, _, address string) (net.Conn, error) {
				attempted.record(address)
				if address == "first:5552" {
					return connection, nil
				}
				return nil, nextFailure
			}

			var env *Environment
			var err error
			if withContext {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				DeferCleanup(cancel)
				env, err = NewEnvironmentWithContext(ctx, options)
			} else {
				env, err = NewEnvironment(options)
			}
			if env != nil {
				DeferCleanup(func() { _ = env.Close() })
			}

			Expect(err).To(MatchError(nextFailure))
			Expect(attempted.recorded()).To(Equal([]string{"first:5552", "second:5552"}))
			Expect(connection.closes.Load()).To(BeNumerically(">", 0),
				"failed handshake socket was not closed")
		},

		Entry("NewEnvironment", false),
		Entry("NewEnvironmentWithContext", true),
	)
})
