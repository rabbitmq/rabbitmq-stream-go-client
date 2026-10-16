package stream

import (
	"bufio"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
)

type shutdownTestWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *shutdownTestWriter) Write(data []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return 0, io.ErrClosedPipe
}

func TestProducerShutdownWaitsForPendingWrites(t *testing.T) {
	for _, maxUnconfirmed := range []int{100, 1} {
		t.Run(fmt.Sprint(maxUnconfirmed), func(t *testing.T) {
			testProducerShutdownWaitsForPendingWrites(t, maxUnconfirmed)
		})
	}
}

func testProducerShutdownWaitsForPendingWrites(t *testing.T, maxUnconfirmed int) {
	client := newClient(connectionParameters{rpcTimeout: defaultSocketCallTimeout})
	producer, err := client.coordinator.NewProducer(NewProducerOptions().SetBatchSize(1), nil)
	if err != nil {
		t.Fatal(err)
	}
	producer.client = client
	producer.unConfirmed = newUnConfirmed(maxUnconfirmed)
	writer := &shutdownTestWriter{started: make(chan struct{}), release: make(chan struct{})}
	client.socket.writer = bufio.NewWriterSize(writer, socketWriteBufferSize)
	confirms := producer.NotifyPublishConfirmation()
	const count = 100
	for i := range count {
		msg := amqp.NewMessage([]byte("shutdown"))
		if err := producer.pendingSequencesQueue.Enqueue(&messageSequence{
			sourceMsg: msg, publishingId: int64(i), messageBytes: []byte("shutdown"),
		}); err != nil {
			t.Fatal(err)
		}
	}
	producer.processPendingSequencesQueue()
	select {
	case <-writer.started:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher did not start writing")
	}
	producer.setStatus(closed)
	results := make(chan []*ConfirmationStatus, count)
	confirmationsDone := make(chan struct{})
	go func() {
		defer close(confirmationsDone)
		for batch := range confirms {
			results <- batch
		}
	}()
	stopped := make(chan struct{})
	go func() {
		producer.stopAndWaitPendingSequencesQueue()
		producer.closeConfirmationStatus()
		close(stopped)
	}()
	select {
	case <-stopped:
		close(writer.release)
		t.Fatal("shutdown finished while the publishing worker was still writing")
	case <-time.After(time.Second):
		close(writer.release)
	}
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not finish after releasing the writer")
	}
	<-confirmationsDone
	close(results)
	seen := make(map[int64]bool, count)
	for batch := range results {
		for _, confirmation := range batch {
			id := confirmation.GetPublishingId()
			if seen[id] {
				t.Errorf("duplicate confirmation for %d", id)
			}
			seen[id] = true
			if confirmation.IsConfirmed() {
				t.Errorf("message %d was confirmed without a broker", id)
			}
		}
	}
	if len(seen) != count {
		t.Errorf("received %d confirmations for %d accepted messages", len(seen), count)
	}
}

func TestProducerSendBlockedOnFullQueueIsReportedOnClose(t *testing.T) {
	client := newClient(connectionParameters{rpcTimeout: defaultSocketCallTimeout})
	producer, err := client.coordinator.NewProducer(NewProducerOptions().SetQueueSize(1), nil)
	if err != nil {
		t.Fatal(err)
	}
	producer.client = client
	confirms := producer.NotifyPublishConfirmation()
	// nothing consumes the queue, so the second Send blocks on the full queue
	if err := producer.Send(amqp.NewMessage([]byte("queued"))); err != nil {
		t.Fatal(err)
	}
	sendErr := make(chan error, 1)
	go func() { sendErr <- producer.Send(amqp.NewMessage([]byte("blocked"))) }()
	time.Sleep(200 * time.Millisecond)

	producer.pendingSequencesQueue.Stop()
	select {
	case err := <-sendErr:
		if err == nil {
			t.Fatal("blocked Send succeeded on a stopped queue")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked Send was not released")
	}
	select {
	case batch := <-confirms:
		if len(batch) != 1 || batch[0].IsConfirmed() {
			t.Fatalf("unexpected confirmations: %v", batch)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rejected message was not reported as unconfirmed")
	}
}
