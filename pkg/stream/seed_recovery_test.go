package stream_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

var _ = Describe("Bootstrap seed recovery", func() {
	It("dials the next seed after a seed that never answers the handshake", func() {
		silentGone := make(chan struct{})
		var silentConns atomic.Int32
		silentPort := fakeSeed(func(conn net.Conn) {
			// Never reply, so the first handshake RPC times out.
			_, _ = io.Copy(io.Discard, conn)
			if silentConns.Add(1) == 1 {
				close(silentGone)
			}
		})

		var refusingAccepts atomic.Int32
		refusingPort := fakeSeed(func(conn net.Conn) {
			refusingAccepts.Add(1)
			_ = conn.Close()
		})

		env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().
			SetUris([]string{seedUri(silentPort), seedUri(refusingPort)}).
			SetRPCTimeout(200 * time.Millisecond))
		if env != nil {
			DeferCleanup(func() { Expect(env.Close()).To(Succeed()) })
		}

		Expect(err).To(HaveOccurred(), "no seed completed the handshake")
		Eventually(refusingAccepts.Load, 5*time.Second).Should(BeEquivalentTo(1),
			"the second seed was never dialed")
		Eventually(silentGone, 5*time.Second).Should(BeClosed(),
			"the failed attempt's socket was left open")
	})

	It("connects through the next seed after a seed closes during the handshake", func() {
		var closingAccepts atomic.Int32
		closingPort := fakeSeed(func(conn net.Conn) {
			closingAccepts.Add(1)
			_ = conn.Close()
		})

		// Relay the second seed to the real broker and record when the client
		// side of the relayed connection goes away.
		var relayAccepts atomic.Int32
		relayClientGone := make(chan struct{})
		relayPort := fakeSeed(func(client net.Conn) {
			if relayAccepts.Add(1) != 1 {
				_ = client.Close()
				return
			}
			defer close(relayClientGone)
			broker, err := net.Dial("tcp", "localhost:5552")
			if err != nil {
				_ = client.Close()
				return
			}
			defer func() { _ = broker.Close() }()
			go func() {
				defer GinkgoRecover()
				_, _ = io.Copy(client, broker)
				_ = client.(*net.TCPConn).CloseWrite()
			}()
			buffer := make([]byte, 4096)
			for {
				n, err := client.Read(buffer)
				if n > 0 {
					_, _ = broker.Write(buffer[:n])
				}
				if err != nil {
					return
				}
			}
		})

		env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().
			SetUris([]string{seedUri(closingPort), seedUri(relayPort)}).
			SetRPCTimeout(time.Second))
		if env != nil {
			DeferCleanup(func() { Expect(env.Close()).To(Succeed()) })
		}

		Expect(err).NotTo(HaveOccurred())
		Expect(closingAccepts.Load()).To(BeEquivalentTo(1))
		Expect(relayAccepts.Load()).To(BeEquivalentTo(1),
			"the second seed was never dialed")
		Eventually(relayClientGone, 5*time.Second).Should(BeClosed(),
			"the bootstrap connection to the second seed was left open")
	})
})

// fakeSeed listens on a loopback port and serves every accepted connection
// in its own goroutine. The listener and the accepted connections are closed
// when the spec ends.
func fakeSeed(serve func(net.Conn)) string {
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	Expect(err).NotTo(HaveOccurred())

	conns := make(chan net.Conn, 16)
	acceptDone := make(chan struct{})
	go func() {
		defer GinkgoRecover()
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			select {
			case conns <- conn:
			default:
			}
			go func() {
				defer GinkgoRecover()
				serve(conn)
			}()
		}
	}()

	DeferCleanup(func() {
		_ = listener.Close()
		<-acceptDone
		for {
			select {
			case conn := <-conns:
				_ = conn.Close()
			default:
				return
			}
		}
	})

	_, port, err := net.SplitHostPort(listener.Addr().String())
	Expect(err).NotTo(HaveOccurred())
	return port
}

func seedUri(port string) string {
	return fmt.Sprintf("rabbitmq-stream://guest:guest@127.0.0.1:%s/", port)
}
