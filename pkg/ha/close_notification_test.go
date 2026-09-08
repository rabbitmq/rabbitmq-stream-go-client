package ha

import (
	"sync"
	"testing"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuperStreamDrainsTerminalPartitionEvents(t *testing.T) {
	r := &ReliableSuperStreamConsumer{consumerOptions: stream.NewSuperStreamConsumerOptions(), mutexStatus: &sync.Mutex{}, status: StatusOpen}
	events := make(chan stream.CPartitionClose, 1)
	r.handleNotifyClose(events)
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		defer close(events)
		// A terminal event followed by unexpected closes must be drained without
		// starting reconnection against this deliberately absent environment.
		events <- stream.CPartitionClose{Event: stream.Event{Reason: stream.UnSubscribe}}
		for range 4 {
			events <- stream.CPartitionClose{Event: stream.Event{Reason: stream.SocketClosed}}
		}
	}()
	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("HA listener abandoned pending partition events")
	}
	require.Eventually(t, func() bool { return r.GetStatus() == StatusClosed }, time.Second, time.Millisecond)
	assert.Equal(t, StatusClosed, r.GetStatus())
}

// Embedding the real reliable consumer retains its shutdown behavior while
// exposing the exact point at which retry enters backoff, without a broker.
type retryCloseConsumer struct {
	*ReliableSuperStreamConsumer
	entered chan struct{}
}

func (r *retryCloseConsumer) getInfo() string {
	select {
	case r.entered <- struct{}{}:
	default:
	}
	return "close-during-retry"
}

func TestSuperStreamCloseInterruptsRetryBackoff(t *testing.T) {
	r := &ReliableSuperStreamConsumer{consumerOptions: stream.NewSuperStreamConsumerOptions(), mutexStatus: &sync.Mutex{}, status: StatusReconnecting, stopRetry: make(chan struct{})}
	r.consumer.Store(&stream.SuperStreamConsumer{})
	observer := &retryCloseConsumer{ReliableSuperStreamConsumer: r, entered: make(chan struct{}, 1)}
	finished := make(chan error, 1)
	go func() {
		err, connected := retry(1, observer, "events-0")
		if connected {
			finished <- nil
		} else {
			finished <- err
		}
	}()
	select {
	case <-observer.entered:
	case <-time.After(time.Second):
		t.Fatal("retry did not enter backoff")
	}
	require.NoError(t, r.Close())
	select {
	case err := <-finished:
		require.ErrorIs(t, err, stream.AlreadyClosed)
	case <-time.After(time.Second):
		t.Fatal("terminal Close did not stop pending retry")
	}
	assert.Equal(t, StatusClosed, r.GetStatus())
	require.NoError(t, r.Close())
}
