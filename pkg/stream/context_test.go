package stream

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContextCancelsBlackholedDial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	options := NewEnvironmentOptions().SetRPCTimeout(time.Hour)
	options.TCPParameters.dialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	done := make(chan error, 1)
	go func() { _, err := NewEnvironmentWithContext(ctx, options); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("dial did not start")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("dial ignored cancellation")
	}
	assert.Nil(t, options.TCPParameters.connectionContext, "constructor mutated caller options")
}

type observedContextConn struct {
	net.Conn
	writes chan struct{}
	once   atomic.Bool
}

func (c *observedContextConn) Write(data []byte) (int, error) {
	if c.once.CompareAndSwap(false, true) {
		close(c.writes)
	}
	return c.Conn.Write(data)
}
func TestContextInterruptsStalledHandshake(t *testing.T) {
	for _, mode := range []string{"write", "response", "tls"} {
		t.Run(mode, func(t *testing.T) {
			client, server := net.Pipe()
			defer func() { _ = server.Close() }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			writes := make(chan struct{})
			readerDone := make(chan struct{})
			if mode != "write" {
				go func() { defer close(readerDone); _, _ = io.Copy(io.Discard, server) }()
			} else {
				close(readerDone)
			}
			uri := "rabbitmq-stream://guest:guest@localhost:5552/"
			if mode == "tls" {
				uri = "rabbitmq-stream+tls://guest:guest@localhost:5551/"
			}
			options := NewEnvironmentOptions().SetUri(uri).SetRPCTimeout(time.Hour)
			options.TCPParameters.dialContext = func(context.Context, string, string) (net.Conn, error) {
				return &observedContextConn{Conn: client, writes: writes}, nil
			}
			done := make(chan error, 1)
			go func() { _, err := NewEnvironmentWithContext(ctx, options); done <- err }()
			select {
			case <-writes:
			case err := <-done:
				t.Fatalf("handshake failed before I/O: %v", err)
			case <-time.After(time.Second):
				t.Fatal("handshake never started")
			}
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("handshake ignored cancellation")
			}
			select {
			case <-readerDone:
			case <-time.After(time.Second):
				t.Fatal("socket reader leaked")
			}
			_, err := server.Write([]byte("after cancellation"))
			assert.Error(t, err)
		})
	}
}

func TestContextCancelsProtocolWaits(t *testing.T) {
	for _, phase := range []string{"code", "data"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			client := &Client{coordinator: NewCoordinator(), tcpParameters: &TCPParameters{connectionContext: ctx}, socketCallTimeout: time.Hour}
			response := newResponse("context-test")
			done := make(chan error, 1)
			go func() {
				if phase == "code" {
					done <- client.waitCode(response).Err
				} else {
					_, err := client.waitData(response)
					done <- err
				}
			}()
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("protocol wait ignored cancellation")
			}
		})
	}
}

func TestContextCancelsReconnectAndClose(t *testing.T) {
	for _, action := range []string{"cancel", "close"} {
		t.Run(action, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{})
			var dials atomic.Int32
			options := NewEnvironmentOptions()
			options.ConnectionParameters = []*Broker{newBrokerDefault()}
			options.TCPParameters.connectionContext = ctx
			options.TCPParameters.dialContext = func(context.Context, string, string) (net.Conn, error) {
				if dials.Add(1) == 1 {
					close(entered)
				}
				return nil, errors.New("injected unavailable broker")
			}
			env := &Environment{options: options, locator: newLocator(nil), cancel: cancel, closeDone: make(chan struct{}), producers: newProducersEnvironment(1, nil), consumers: newConsumersEnvironment(1, nil)}
			defer func() { _ = env.Close() }()
			done := make(chan error, 1)
			go func() { done <- env.maybeReconnectLocator() }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("reconnect never started")
			}
			closed := make(chan error, 1)
			if action == "cancel" {
				cancel()
			} else {
				go func() { closed <- env.Close() }()
			}
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("reconnect delay ignored cancellation")
			}
			if action == "close" {
				select {
				case err := <-closed:
					require.NoError(t, err)
				case <-time.After(time.Second):
					t.Fatal("Close blocked behind reconnect")
				}
			}
			assert.EqualValues(t, 1, dials.Load())
		})
	}
}

// metadataCancelConn completes the first metadata response without a leader,
// then cancels while the second response is outstanding.
type metadataCancelConn struct {
	net.Conn
	client   *Client
	cancel   context.CancelFunc
	requests int
	leader   *Broker
}

func (c *metadataCancelConn) Write(data []byte) (int, error) {
	c.requests++
	if c.requests > 1 {
		c.cancel()
		return len(data), nil
	}
	response, err := c.client.coordinator.GetResponseById(binary.BigEndian.Uint32(data[8:12]))
	if err != nil {
		return 0, err
	}
	metadata := StreamsMetadata{}.New()
	metadata.Add("context-metadata", responseCodeOk, c.leader, nil)
	response.code <- Code{id: responseCodeOk}
	response.data <- metadata
	return len(data), nil
}

func TestContextCancelsMetadataLeaderRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newClient(connectionParameters{tcpParameters: &TCPParameters{connectionContext: ctx}, rpcTimeout: time.Hour})
	local, peer := net.Pipe()
	defer func() { _ = local.Close(); _ = peer.Close() }()
	connection := &metadataCancelConn{Conn: local, client: client, cancel: cancel}
	client.setSocketConnection(connection)
	client.socket.setOpen()
	defer client.coordinator.Close()
	env := &Environment{options: &EnvironmentOptions{TCPParameters: client.tcpParameters}, locator: newLocator(client)}
	done := make(chan error, 1)
	go func() { _, err := env.StreamMetaData("context-metadata"); done <- err }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("metadata retry ignored cancellation")
	}
	assert.Equal(t, 2, connection.requests)
}

func TestContextCancelsAdvertisedDNSLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	parameters := &TCPParameters{connectionContext: ctx, lookupIPAddr: func(ctx context.Context, _ string) ([]net.IPAddr, error) {
		close(entered)
		<-ctx.Done()
		return nil, &net.DNSError{Err: ctx.Err().Error(), IsTimeout: true}
	}}
	client := newClient(connectionParameters{tcpParameters: parameters, rpcTimeout: time.Hour})
	local, peer := net.Pipe()
	defer func() { _ = local.Close(); _ = peer.Close() }()
	client.setSocketConnection(&metadataCancelConn{Conn: local, client: client, cancel: cancel, leader: &Broker{Host: "advertised.invalid", Port: "5552"}})
	client.socket.setOpen()
	defer client.coordinator.Close()
	done := make(chan error, 1)
	go func() { _, err := client.BrokerLeader("context-metadata"); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("advertised DNS lookup did not start")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("advertised DNS lookup ignored lifetime cancellation")
	}
}

func TestContextDiscardsAbandonedResponses(t *testing.T) {
	for _, phase := range []string{"canceled-write", "failed-write", "data", "tune"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := newClient(connectionParameters{tcpParameters: &TCPParameters{connectionContext: ctx}, rpcTimeout: time.Hour})
			local, peer := net.Pipe()
			defer func() { _ = local.Close(); _ = peer.Close() }()
			client.setSocketConnection(local)
			defer client.coordinator.Close()
			response := client.coordinator.NewResponse(commandMetadata)
			switch phase {
			case "canceled-write":
				cancel()
				require.ErrorIs(t, client.handleWrite([]byte("request"), response).Err, context.Canceled)
			case "failed-write":
				require.NoError(t, peer.Close())
				require.Error(t, client.handleWrite([]byte("request"), response).Err)
			case "data":
				cancel()
				_, err := client.waitData(response)
				require.ErrorIs(t, err, context.Canceled)
			case "tune":
				client.coordinator.discardResponse(response)
				cancel()
				require.ErrorIs(t, client.sendSaslAuthenticate("PLAIN", nil), context.Canceled)
			}
			assert.Empty(t, client.coordinator.responses)
			// Abandonment must not close channels that an in-flight reader captured.
			response.code <- Code{id: responseCodeOk}
			response.data <- "late response"
		})
	}
}
