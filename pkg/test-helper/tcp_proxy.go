package test_helper

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"strconv"
	"sync"
)

// TCPProxy is a TCP proxy placed between the client and the broker.
// It allows tests to simulate network failures (dropped connections, broker
// unreachable) and server-side frames (metadata update) without using the
// broker HTTP management API.
//
// Mutex usage: mu protects pairs, refuse and closed only, it is never held during network I/O.
type TCPProxy struct {
	target string

	mu       sync.Mutex
	listener net.Listener
	addr     string
	pairs    map[*proxyPair]struct{}
	closed   bool
}

type proxyPair struct {
	client net.Conn
	server net.Conn
	// writeToClient serializes the frames sent to the client, so injected
	// frames never interleave with the frames coming from the broker
	writeToClient sync.Mutex
}

// NewTCPProxy starts a proxy on a random local port forwarding to target (host:port).
func NewTCPProxy(target string) (*TCPProxy, error) {
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &TCPProxy{listener: l, addr: l.Addr().String(), target: target, pairs: map[*proxyPair]struct{}{}}
	go p.acceptLoop(l)
	return p, nil
}

// Port returns the local port the client has to connect to.
func (p *TCPProxy) Port() int {
	_, port, _ := net.SplitHostPort(p.addr)
	n, _ := strconv.Atoi(port)
	return n
}

func (p *TCPProxy) snapshot() []*proxyPair {
	p.mu.Lock()
	defer p.mu.Unlock()
	pairs := make([]*proxyPair, 0, len(p.pairs))
	for pair := range p.pairs {
		pairs = append(pairs, pair)
	}
	return pairs
}

// DropConnections closes all the open connections, like a network failure.
// New connections are still accepted, so the client can reconnect immediately.
func (p *TCPProxy) DropConnections() {
	for _, pair := range p.snapshot() {
		pair.close()
	}
}

// Outage drops all the connections and stops listening until Restore is called,
// like a broker that is temporarily unreachable: the new connections are refused.
func (p *TCPProxy) Outage() {
	p.mu.Lock()
	if p.listener != nil {
		_ = p.listener.Close()
		p.listener = nil
	}
	p.mu.Unlock()
	p.DropConnections()
}

// Restore listens again on the same port after an Outage.
func (p *TCPProxy) Restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.listener != nil {
		return
	}
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", p.addr)
	if err != nil {
		return
	}
	p.listener = l
	go p.acceptLoop(l)
}

// SendMetadataUpdate sends to all the clients a metadata update frame
// with code "stream not available" for the stream, as the broker does when the
// stream leader changes or the stream is restarted.
func (p *TCPProxy) SendMetadataUpdate(streamName string) {
	const (
		commandMetadataUpdate          = 0x0010
		responseCodeStreamNotAvailable = 6
	)
	frame := make([]byte, 0, 14+len(streamName))
	frame = binary.BigEndian.AppendUint32(frame, uint32(8+len(streamName)))
	frame = binary.BigEndian.AppendUint16(frame, commandMetadataUpdate)
	frame = binary.BigEndian.AppendUint16(frame, 1) // version
	frame = binary.BigEndian.AppendUint16(frame, responseCodeStreamNotAvailable)
	frame = binary.BigEndian.AppendUint16(frame, uint16(len(streamName)))
	frame = append(frame, streamName...)

	for _, pair := range p.snapshot() {
		pair.writeToClient.Lock()
		_, _ = pair.client.Write(frame)
		pair.writeToClient.Unlock()
	}
}

// Close stops the proxy and closes all the connections.
func (p *TCPProxy) Close() {
	p.mu.Lock()
	p.closed = true
	if p.listener != nil {
		_ = p.listener.Close()
		p.listener = nil
	}
	p.mu.Unlock()
	p.DropConnections()
}

func (p *TCPProxy) acceptLoop(l net.Listener) {
	for {
		client, err := l.Accept()
		if err != nil {
			return
		}
		server, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		pair := &proxyPair{client: client, server: server}
		p.mu.Lock()
		p.pairs[pair] = struct{}{}
		p.mu.Unlock()
		go func() {
			_, _ = io.Copy(server, client)
			pair.close()
			p.remove(pair)
		}()
		go func() {
			pair.copyFramesToClient()
			pair.close()
			p.remove(pair)
		}()
	}
}

func (p *TCPProxy) remove(pair *proxyPair) {
	p.mu.Lock()
	delete(p.pairs, pair)
	p.mu.Unlock()
}

// copyFramesToClient forwards whole frames, so injected frames
// can be written between them without corrupting the stream.
func (pp *proxyPair) copyFramesToClient() {
	header := make([]byte, 4)
	for {
		if _, err := io.ReadFull(pp.server, header); err != nil {
			return
		}
		frame := make([]byte, 4+binary.BigEndian.Uint32(header))
		copy(frame, header)
		if _, err := io.ReadFull(pp.server, frame[4:]); err != nil {
			return
		}
		pp.writeToClient.Lock()
		_, err := pp.client.Write(frame)
		pp.writeToClient.Unlock()
		if err != nil {
			return
		}
	}
}

func (pp *proxyPair) close() {
	_ = pp.client.Close()
	_ = pp.server.Close()
}
