package stream

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests use the coordinator to reproduce close-before-registration without
// needing a broker or relying on a network failure arriving at a particular time.
func TestProducerCloseBeforeNotificationRegistration(t *testing.T) {
	producer, err := NewCoordinator().NewProducer(nil, nil)
	require.NoError(t, err)
	require.NoError(t, producer.close(Event{Reason: SocketClosed}))
	events := producer.NotifyClose()
	assert.Equal(t, events, producer.NotifyClose())
	select {
	case event, ok := <-events:
		require.True(t, ok)
		assert.Equal(t, SocketClosed, event.Reason)
	case <-time.After(time.Second):
		t.Fatal("early producer close event was lost")
	}
	_, ok := <-events
	assert.False(t, ok)
}

func TestConsumerCloseBeforeNotificationRegistration(t *testing.T) {
	consumer, err := NewCoordinator().NewConsumer(nil, NewConsumerOptions(), nil)
	require.NoError(t, err)
	consumer.close(Event{Reason: SocketClosed})
	events := consumer.NotifyClose()
	assert.Equal(t, events, consumer.NotifyClose())
	select {
	case event, ok := <-events:
		require.True(t, ok)
		assert.Equal(t, SocketClosed, event.Reason)
	case <-time.After(time.Second):
		t.Fatal("early consumer close event was lost")
	}
	_, ok := <-events
	assert.False(t, ok)
}

func TestProducerConcurrentCloseHasOneOwner(t *testing.T) {
	for range 100 {
		producer, err := NewCoordinator().NewProducer(nil, nil)
		require.NoError(t, err)
		const closers = 32
		start := make(chan struct{})
		results := make(chan error, closers)
		for range closers {
			go func() { <-start; results <- producer.close(Event{Reason: SocketClosed}) }()
		}
		close(start)
		owners := 0
		for range closers {
			select {
			case err := <-results:
				if err == nil {
					owners++
				} else {
					require.ErrorIs(t, err, AlreadyClosed)
				}
			case <-time.After(time.Second):
				t.Fatal("concurrent producer close blocked")
			}
		}
		assert.Equal(t, 1, owners)
		_, ok := <-producer.NotifyClose()
		assert.True(t, ok)
		_, ok = <-producer.NotifyClose()
		assert.False(t, ok)
	}
}

func TestSuperStreamRetainsPartitionEventsBeforeRegistration(t *testing.T) {
	consumer := &SuperStreamConsumer{stopPartitionForwarding: make(chan struct{})}
	for _, partition := range []string{"events-0", "events-1"} {
		consumer.notifyPartitionClose(CPartitionClose{Partition: partition, Event: Event{Reason: SocketClosed}})
	}
	require.NoError(t, consumer.Close())
	require.Eventually(t, func() bool {
		consumer.chSuperStreamPartitionMutex.Lock()
		defer consumer.chSuperStreamPartitionMutex.Unlock()
		return consumer.partitionNotificationsClosed
	}, time.Second, time.Millisecond)
	events := consumer.NotifyPartitionClose(1)
	assert.Equal(t, events, consumer.NotifyPartitionClose(10))
	partitions := make([]string, 0, 2)
	for event := range events {
		partitions = append(partitions, event.Partition)
	}
	assert.Equal(t, []string{"events-0", "events-1"}, partitions)
	require.NoError(t, consumer.Close())
	require.ErrorIs(t, consumer.ConnectPartition("events-0", OffsetSpecification{}.First()), AlreadyClosed)
}

func TestSuperStreamCloseWaitsForPartitionForwarders(t *testing.T) {
	consumer := &SuperStreamConsumer{stopPartitionForwarding: make(chan struct{})}
	events := consumer.NotifyPartitionClose(1)
	consumer.partitionCloseWorkers.Add(1)
	started := make(chan struct{})
	go func() {
		defer consumer.partitionCloseWorkers.Done()
		close(started)
		consumer.notifyPartitionClose(CPartitionClose{Partition: "events-0"})
	}()
	<-started
	require.NoError(t, consumer.Close())
	// A slow reader must not lose its event to a timer-based channel close.
	select {
	case event, ok := <-events:
		require.True(t, ok)
		assert.Equal(t, "events-0", event.Partition)
	case <-time.After(time.Second):
		t.Fatal("partition forwarder did not complete")
	}
	select {
	case _, ok := <-events:
		assert.False(t, ok)
	case <-time.After(time.Second):
		t.Fatal("partition notification channel was not closed")
	}
}

func TestSuperStreamConcurrentNotificationRegistration(t *testing.T) {
	consumer := &SuperStreamConsumer{stopPartitionForwarding: make(chan struct{})}
	consumer.partitionCloseWorkers.Add(1)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { consumer.NotifyPartitionClose(64) })
	}
	wg.Go(func() {
		defer consumer.partitionCloseWorkers.Done()
		consumer.notifyPartitionClose(CPartitionClose{Partition: "events-0"})
	})
	wg.Wait()
	assert.GreaterOrEqual(t, cap(consumer.NotifyPartitionClose(1)), 64)
	require.NoError(t, consumer.Close())
	events := make([]CPartitionClose, 0, 1)
	for event := range consumer.NotifyPartitionClose(1) {
		events = append(events, event)
	}
	require.Len(t, events, 1)
	assert.Equal(t, "events-0", events[0].Partition)
}

func TestSuperStreamCloseDoesNotWaitForAbandonedReader(t *testing.T) {
	consumer := &SuperStreamConsumer{stopPartitionForwarding: make(chan struct{})}
	events := consumer.NotifyPartitionClose(1)
	consumer.notifyPartitionClose(CPartitionClose{Partition: "old-event"})
	consumer.partitionCloseWorkers.Add(1)
	go func() {
		defer consumer.partitionCloseWorkers.Done()
		consumer.notifyPartitionClose(CPartitionClose{Partition: "shutdown-event"})
	}()
	require.NoError(t, consumer.Close())
	require.Eventually(t, func() bool {
		consumer.chSuperStreamPartitionMutex.Lock()
		defer consumer.chSuperStreamPartitionMutex.Unlock()
		return consumer.partitionNotificationsClosed
	}, time.Second, time.Millisecond)
	event, ok := <-events
	require.True(t, ok)
	assert.Equal(t, "old-event", event.Partition)
	_, ok = <-events
	assert.False(t, ok)
}
