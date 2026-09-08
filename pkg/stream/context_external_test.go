package stream_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvironmentContextRejectsInvalidLifetime(t *testing.T) {
	_, err := stream.NewEnvironmentWithContext(nil, nil) //nolint:staticcheck // Verify the public constructor rejects invalid contexts.
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = stream.NewEnvironmentWithContext(ctx, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestEnvironmentContextHandshakeDeadline(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		peer, err := listener.Accept()
		if err != nil {
			return
		}
		defer func() { _ = peer.Close() }()
		_, _ = io.Copy(io.Discard, peer)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = stream.NewEnvironmentWithContext(ctx, stream.NewEnvironmentOptions().SetUri("rabbitmq-stream://guest:guest@"+listener.Addr().String()+"/").SetRPCTimeout(time.Hour))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(started), 2*time.Second)
	select {
	case <-peerDone:
	case <-time.After(time.Second):
		t.Fatal("deadline did not release the handshake socket")
	}
}

// The upstream suite provides RabbitMQ on the default local listener.
func TestEnvironmentLifetimeContext(t *testing.T) {
	for _, action := range []string{"cancel", "close"} {
		t.Run(action, func(t *testing.T) {
			cleanup, err := stream.NewEnvironment(nil)
			require.NoError(t, err)
			defer func() { _ = cleanup.Close() }()
			name := fmt.Sprintf("context-lifetime-%d", time.Now().UnixNano())
			require.NoError(t, cleanup.DeclareStream(name, nil))
			defer func() { _ = cleanup.DeleteStream(name) }()
			options := stream.NewEnvironmentOptions().SetRPCTimeout(time.Hour)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			env, err := stream.NewEnvironmentWithContext(ctx, options)
			require.NoError(t, err)
			defer func() { _ = env.Close() }()
			producer, err := env.NewProducer(name, nil)
			require.NoError(t, err)
			confirmations := producer.NotifyPublishConfirmation()
			closeEvents := producer.NotifyClose()
			require.NoError(t, producer.Send(amqp.NewMessage([]byte("before cancellation"))))
			select {
			case batch := <-confirmations:
				require.Len(t, batch, 1)
				require.True(t, batch[0].IsConfirmed())
			case <-time.After(3 * time.Second):
				t.Fatal("publication not confirmed")
			}
			done := make(chan error, 1)
			if action == "cancel" {
				cancel()
				go func() { done <- env.Close() }()
			} else {
				go func() { done <- env.Close() }()
			}
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("Close ignored canceled environment")
			}
			select {
			case <-closeEvents:
			case <-time.After(time.Second):
				t.Fatal("producer socket remained active")
			}
			require.True(t, env.IsClosed())
			_, err = env.QueryPartitions(name)
			require.ErrorIs(t, err, context.Canceled)
			// Reusing the caller's options starts an independent lifetime.
			next, err := stream.NewEnvironmentWithContext(context.Background(), options)
			require.NoError(t, err)
			require.NoError(t, next.Close())
		})
	}
}

// A verified TLS connection must infer the URI hostname without changing the
// caller's reusable TLS configuration.
func TestEnvironmentContextTLSHostname(t *testing.T) {
	ca, err := os.ReadFile("../../.ci/certs/ca_certificate.pem")
	require.NoError(t, err)
	roots := x509.NewCertPool()
	require.True(t, roots.AppendCertsFromPEM(ca))
	certificate, err := tls.LoadX509KeyPair("../../.ci/certs/server_certificate.pem", "../../.ci/certs/server_key.pem")
	require.NoError(t, err)
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	handshake := make(chan error, 1)
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		peer, acceptErr := listener.Accept()
		if acceptErr != nil {
			handshake <- acceptErr
			return
		}
		defer func() { _ = peer.Close() }()
		secured := tls.Server(peer, &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})
		handshake <- secured.HandshakeContext(context.Background())
		_, _ = io.Copy(io.Discard, secured)
	}()
	config := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, port, _ := net.SplitHostPort(listener.Addr().String())
		_, err := stream.NewEnvironmentWithContext(ctx, stream.NewEnvironmentOptions().SetUri("rabbitmq-stream+tls://guest:guest@localhost:"+port+"/").SetTLSConfig(config))
		done <- err
	}()
	select {
	case err := <-handshake:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("verified TLS handshake did not finish")
	}
	assert.Empty(t, config.ServerName)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("TLS protocol wait did not stop")
	}
	select {
	case <-peerDone:
	case <-time.After(time.Second):
		t.Fatal("TLS peer leaked after cancellation")
	}
}
