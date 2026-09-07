package stream

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"go.opentelemetry.io/otel/metric/noop"
)

// deliverFrame builds the body of one Deliver frame -- everything
// handleDeliver reads, starting at the subscription id -- carrying records
// plain (uncompressed, unfiltered) entries of bodySize bytes each.
func deliverFrame(tb testing.TB, records int, bodySize int) []byte {
	tb.Helper()

	encoded, err := amqp.NewMessage(bytes.Repeat([]byte("x"), bodySize)).MarshalBinary()
	if err != nil {
		tb.Fatal(err)
	}

	payload := &bytes.Buffer{}
	for range records {
		writeUInt(payload, uint32(len(encoded)))
		payload.Write(encoded)
	}

	frame := &bytes.Buffer{}
	writeByte(frame, 0)                 // subscription id
	writeByte(frame, 0)                 // magic and version, skipped
	writeByte(frame, 0)                 // chunk type
	writeUShort(frame, uint16(records)) // num entries
	writeUInt(frame, uint32(records))   // num records
	writeLong(frame, time.Now().Unix()) // timestamp
	writeLong(frame, 0)                 // epoch
	writeLong(frame, 0)                 // offset
	writeUInt(frame, crc32.ChecksumIEEE(payload.Bytes()))
	writeUInt(frame, uint32(payload.Len()))
	writeUInt(frame, 0) // reserved
	writeUInt(frame, 0) // reserved
	frame.Write(payload.Bytes())

	return frame.Bytes()
}

// BenchmarkHandleDeliver prices one delivered chunk on the consumer side: the
// payload buffer, the readers over it, the decoded messages and the metric
// attributes. It is the per-message cost of consuming, so the allocations
// here are the ones a busy consumer pays for every message off the wire.
func BenchmarkHandleDeliver(b *testing.B) {
	for _, records := range []int{1, 20, 100} {
		b.Run(fmt.Sprintf("records=%d", records), func(b *testing.B) {
			metrics, err := newStreamMetrics(noop.NewMeterProvider())
			if err != nil {
				b.Fatal(err)
			}
			client := newClient(connectionParameters{
				broker:     newBrokerDefault(),
				rpcTimeout: time.Second,
				metrics:    metrics,
			})
			consumer, err := client.coordinator.NewConsumer(nil, &ConsumerOptions{
				streamName:     "bench",
				Offset:         OffsetSpecification{}.Last(),
				initialCredits: 10,
				CRCCheck:       true,
			}, func() {})
			if err != nil {
				b.Fatal(err)
			}

			// The dispatch goroutine is not running, so the chunks have to be
			// taken off the consumer's channel here or handleDeliver blocks.
			done := make(chan struct{})
			defer close(done)
			go func() {
				for {
					select {
					case <-consumer.chunkForConsumer:
					case <-done:
						return
					}
				}
			}()

			frame := deliverFrame(b, records, 1024)
			source := bytes.NewReader(frame)
			reader := bufio.NewReader(source)

			b.ReportAllocs()
			b.SetBytes(int64(len(frame)))
			b.ResetTimer()
			for b.Loop() {
				source.Reset(frame)
				reader.Reset(source)
				client.handleDeliver(reader)
			}
		})
	}
}
