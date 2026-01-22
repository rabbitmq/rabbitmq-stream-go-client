package stream

import (
	"context"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// Mock infrastructure for testing metrics without OTEL SDK

type metricRecord struct {
	name       string
	value      int64
	attributes attribute.Set
}

type mockInt64Counter struct {
	metric.Int64Counter
	name    string
	records []metricRecord
	mu      sync.Mutex
}

func (m *mockInt64Counter) Add(ctx context.Context, incr int64, options ...metric.AddOption) {
	m.mu.Lock()
	defer m.mu.Unlock()

	config := metric.NewAddConfig(options)
	m.records = append(m.records, metricRecord{
		name:       m.name,
		value:      incr,
		attributes: config.Attributes(),
	})
}

func (m *mockInt64Counter) getRecords() []metricRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]metricRecord{}, m.records...)
}

func (m *mockInt64Counter) getTotalValue() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var total int64
	for _, r := range m.records {
		total += r.value
	}
	return total
}

type mockInt64UpDownCounter struct {
	metric.Int64UpDownCounter
	name    string
	records []metricRecord
	mu      sync.Mutex
}

func (m *mockInt64UpDownCounter) Add(ctx context.Context, incr int64, options ...metric.AddOption) {
	m.mu.Lock()
	defer m.mu.Unlock()

	config := metric.NewAddConfig(options)
	m.records = append(m.records, metricRecord{
		name:       m.name,
		value:      incr,
		attributes: config.Attributes(),
	})
}

func (m *mockInt64UpDownCounter) getRecords() []metricRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]metricRecord{}, m.records...)
}

func (m *mockInt64UpDownCounter) getTotalValue() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var total int64
	for _, r := range m.records {
		total += r.value
	}
	return total
}

type mockInt64Histogram struct {
	metric.Int64Histogram
	name    string
	records []metricRecord
	mu      sync.Mutex
}

func (m *mockInt64Histogram) Record(ctx context.Context, incr int64, options ...metric.RecordOption) {
	m.mu.Lock()
	defer m.mu.Unlock()

	config := metric.NewRecordConfig(options)
	m.records = append(m.records, metricRecord{
		name:       m.name,
		value:      incr,
		attributes: config.Attributes(),
	})
}

func (m *mockInt64Histogram) getRecords() []metricRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]metricRecord{}, m.records...)
}

type mockMeter struct {
	metric.Meter
	counters       map[string]*mockInt64Counter
	upDownCounters map[string]*mockInt64UpDownCounter
	histograms     map[string]*mockInt64Histogram
	mu             sync.Mutex
}

func newMockMeter() *mockMeter {
	return &mockMeter{
		Meter:          noop.Meter{},
		counters:       make(map[string]*mockInt64Counter),
		upDownCounters: make(map[string]*mockInt64UpDownCounter),
		histograms:     make(map[string]*mockInt64Histogram),
	}
}

func (m *mockMeter) Int64Counter(name string, options ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	counter := &mockInt64Counter{
		Int64Counter: noop.Int64Counter{},
		name:         name,
		records:      make([]metricRecord, 0),
	}
	m.counters[name] = counter
	return counter, nil
}

func (m *mockMeter) Int64UpDownCounter(name string, options ...metric.Int64UpDownCounterOption) (metric.Int64UpDownCounter, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	counter := &mockInt64UpDownCounter{
		Int64UpDownCounter: noop.Int64UpDownCounter{},
		name:               name,
		records:            make([]metricRecord, 0),
	}
	m.upDownCounters[name] = counter
	return counter, nil
}

func (m *mockMeter) Int64Histogram(name string, options ...metric.Int64HistogramOption) (metric.Int64Histogram, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	histogram := &mockInt64Histogram{
		Int64Histogram: noop.Int64Histogram{},
		name:           name,
		records:        make([]metricRecord, 0),
	}
	m.histograms[name] = histogram
	return histogram, nil
}

func (m *mockMeter) getCounter(name string) *mockInt64Counter {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name]
}

func (m *mockMeter) getUpDownCounter(name string) *mockInt64UpDownCounter {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.upDownCounters[name]
}

func (m *mockMeter) getHistogram(name string) *mockInt64Histogram {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.histograms[name]
}

type mockMeterProvider struct {
	metric.MeterProvider
	meter *mockMeter
}

func newMockMeterProvider() *mockMeterProvider {
	return &mockMeterProvider{
		MeterProvider: noop.NewMeterProvider(),
		meter:         newMockMeter(),
	}
}

func (m *mockMeterProvider) Meter(name string, opts ...metric.MeterOption) metric.Meter {
	return m.meter
}

var _ = Describe("Metrics Unit Tests", func() {
	var (
		mockProvider *mockMeterProvider
		metrics      *streamMetrics
	)

	BeforeEach(func() {
		mockProvider = newMockMeterProvider()
		var err error
		metrics, err = newStreamMetrics(mockProvider)
		Expect(err).NotTo(HaveOccurred())
		Expect(metrics).NotTo(BeNil())
	})

	Describe("Metric Creation", func() {
		It("should create all required metric instruments", func() {
			Expect(mockProvider.meter.getCounter("rabbitmq.stream.published")).NotTo(BeNil())
			Expect(mockProvider.meter.getCounter("rabbitmq.stream.confirmed")).NotTo(BeNil())
			Expect(mockProvider.meter.getCounter("rabbitmq.stream.errored")).NotTo(BeNil())
			Expect(mockProvider.meter.getCounter("rabbitmq.stream.chunks")).NotTo(BeNil())
			Expect(mockProvider.meter.getCounter("rabbitmq.stream.consumed")).NotTo(BeNil())
			Expect(mockProvider.meter.getUpDownCounter("rabbitmq.stream.connections")).NotTo(BeNil())
			Expect(mockProvider.meter.getUpDownCounter("rabbitmq.stream.outstanding_publish_confirmations")).NotTo(BeNil())
			Expect(mockProvider.meter.getHistogram("rabbitmq.stream.chunk_size")).NotTo(BeNil())
		})
	})

	Describe("Connection Metrics", func() {
		It("should record connection opened", func() {
			attrs := attribute.NewSet(attribute.String("test", "value"))
			metrics.connectionOpened(context.Background(), attrs)

			counter := mockProvider.meter.getUpDownCounter("rabbitmq.stream.connections")
			Expect(counter).NotTo(BeNil())
			Expect(counter.getTotalValue()).To(Equal(int64(1)))

			records := counter.getRecords()
			Expect(records).To(HaveLen(1))
			Expect(records[0].value).To(Equal(int64(1)))
			Expect(records[0].attributes).To(Equal(attrs))
		})

		It("should record connection closed", func() {
			attrs := attribute.NewSet(attribute.String("test", "value"))
			metrics.connectionClosed(context.Background(), attrs)

			counter := mockProvider.meter.getUpDownCounter("rabbitmq.stream.connections")
			Expect(counter).NotTo(BeNil())
			Expect(counter.getTotalValue()).To(Equal(int64(-1)))

			records := counter.getRecords()
			Expect(records).To(HaveLen(1))
			Expect(records[0].value).To(Equal(int64(-1)))
			Expect(records[0].attributes).To(Equal(attrs))
		})

		It("should handle multiple connection events", func() {
			attrs := attribute.NewSet(attribute.String("server", "localhost"))

			metrics.connectionOpened(context.Background(), attrs)
			metrics.connectionOpened(context.Background(), attrs)
			metrics.connectionClosed(context.Background(), attrs)

			counter := mockProvider.meter.getUpDownCounter("rabbitmq.stream.connections")
			Expect(counter.getTotalValue()).To(Equal(int64(1)))
			Expect(counter.getRecords()).To(HaveLen(3))
		})
	})

	Describe("Producer Metrics", func() {
		It("should record published messages", func() {
			attrs := attribute.NewSet(attribute.String("stream", "test-stream"))
			metrics.published(context.Background(), 10, attrs)

			publishedCounter := mockProvider.meter.getCounter("rabbitmq.stream.published")
			Expect(publishedCounter).NotTo(BeNil())
			Expect(publishedCounter.getTotalValue()).To(Equal(int64(10)))

			outstandingCounter := mockProvider.meter.getUpDownCounter("rabbitmq.stream.outstanding_publish_confirmations")
			Expect(outstandingCounter).NotTo(BeNil())
			Expect(outstandingCounter.getTotalValue()).To(Equal(int64(10)))

			records := publishedCounter.getRecords()
			Expect(records).To(HaveLen(1))
			Expect(records[0].value).To(Equal(int64(10)))
			Expect(records[0].attributes).To(Equal(attrs))
		})

		It("should record confirmed messages", func() {
			attrs := attribute.NewSet(attribute.String("stream", "test-stream"))
			metrics.confirmed(context.Background(), 5, attrs)

			confirmedCounter := mockProvider.meter.getCounter("rabbitmq.stream.confirmed")
			Expect(confirmedCounter).NotTo(BeNil())
			Expect(confirmedCounter.getTotalValue()).To(Equal(int64(5)))

			outstandingCounter := mockProvider.meter.getUpDownCounter("rabbitmq.stream.outstanding_publish_confirmations")
			Expect(outstandingCounter).NotTo(BeNil())
			Expect(outstandingCounter.getTotalValue()).To(Equal(int64(-5)))

			records := confirmedCounter.getRecords()
			Expect(records).To(HaveLen(1))
			Expect(records[0].value).To(Equal(int64(5)))
			Expect(records[0].attributes).To(Equal(attrs))
		})

		It("should record errored messages", func() {
			attrs := attribute.NewSet(attribute.String("stream", "test-stream"))
			metrics.errored(context.Background(), 3, attrs)

			erroredCounter := mockProvider.meter.getCounter("rabbitmq.stream.errored")
			Expect(erroredCounter).NotTo(BeNil())
			Expect(erroredCounter.getTotalValue()).To(Equal(int64(3)))

			outstandingCounter := mockProvider.meter.getUpDownCounter("rabbitmq.stream.outstanding_publish_confirmations")
			Expect(outstandingCounter).NotTo(BeNil())
			Expect(outstandingCounter.getTotalValue()).To(Equal(int64(-3)))

			records := erroredCounter.getRecords()
			Expect(records).To(HaveLen(1))
			Expect(records[0].value).To(Equal(int64(3)))
			Expect(records[0].attributes).To(Equal(attrs))
		})

		It("should track outstanding confirmations correctly", func() {
			attrs := attribute.NewSet(attribute.String("stream", "test-stream"))

			// Publish 100 messages
			metrics.published(context.Background(), 100, attrs)
			outstandingCounter := mockProvider.meter.getUpDownCounter("rabbitmq.stream.outstanding_publish_confirmations")
			Expect(outstandingCounter.getTotalValue()).To(Equal(int64(100)))

			// Confirm 60 messages
			metrics.confirmed(context.Background(), 60, attrs)
			Expect(outstandingCounter.getTotalValue()).To(Equal(int64(40)))

			// 10 messages error
			metrics.errored(context.Background(), 10, attrs)
			Expect(outstandingCounter.getTotalValue()).To(Equal(int64(30)))

			// Confirm remaining
			metrics.confirmed(context.Background(), 30, attrs)
			Expect(outstandingCounter.getTotalValue()).To(Equal(int64(0)))
		})
	})

	Describe("Consumer Metrics", func() {
		It("should record consumed messages", func() {
			attrs := attribute.NewSet(attribute.String("stream", "test-stream"))
			metrics.consumed(context.Background(), 20, attrs)

			consumedCounter := mockProvider.meter.getCounter("rabbitmq.stream.consumed")
			Expect(consumedCounter).NotTo(BeNil())
			Expect(consumedCounter.getTotalValue()).To(Equal(int64(20)))

			records := consumedCounter.getRecords()
			Expect(records).To(HaveLen(1))
			Expect(records[0].value).To(Equal(int64(20)))
			Expect(records[0].attributes).To(Equal(attrs))
		})

		It("should record chunk received with size", func() {
			attrs := attribute.NewSet(attribute.String("stream", "test-stream"))
			metrics.chunkReceived(context.Background(), 50, attrs)

			chunksCounter := mockProvider.meter.getCounter("rabbitmq.stream.chunks")
			Expect(chunksCounter).NotTo(BeNil())
			Expect(chunksCounter.getTotalValue()).To(Equal(int64(1)))

			chunkSizeHistogram := mockProvider.meter.getHistogram("rabbitmq.stream.chunk_size")
			Expect(chunkSizeHistogram).NotTo(BeNil())

			records := chunkSizeHistogram.getRecords()
			Expect(records).To(HaveLen(1))
			Expect(records[0].value).To(Equal(int64(50)))
			Expect(records[0].attributes).To(Equal(attrs))
		})

		It("should record multiple chunks with different sizes", func() {
			attrs := attribute.NewSet(attribute.String("stream", "test-stream"))

			metrics.chunkReceived(context.Background(), 10, attrs)
			metrics.chunkReceived(context.Background(), 25, attrs)
			metrics.chunkReceived(context.Background(), 100, attrs)

			chunksCounter := mockProvider.meter.getCounter("rabbitmq.stream.chunks")
			Expect(chunksCounter.getTotalValue()).To(Equal(int64(3)))

			chunkSizeHistogram := mockProvider.meter.getHistogram("rabbitmq.stream.chunk_size")
			records := chunkSizeHistogram.getRecords()
			Expect(records).To(HaveLen(3))
			Expect(records[0].value).To(Equal(int64(10)))
			Expect(records[1].value).To(Equal(int64(25)))
			Expect(records[2].value).To(Equal(int64(100)))
		})
	})

	Describe("Metric Attributes", func() {
		It("should preserve attributes on all metrics", func() {
			attrs := attribute.NewSet(
				attribute.String("server.address", "localhost"),
				attribute.Int("server.port", 5552),
				attribute.String("messaging.system", "rabbitmq"),
			)

			metrics.connectionOpened(context.Background(), attrs)
			metrics.published(context.Background(), 1, attrs)
			metrics.confirmed(context.Background(), 1, attrs)
			metrics.consumed(context.Background(), 1, attrs)
			metrics.chunkReceived(context.Background(), 1, attrs)

			// Verify each metric has the correct attributes
			connRecords := mockProvider.meter.getUpDownCounter("rabbitmq.stream.connections").getRecords()
			Expect(connRecords[0].attributes).To(Equal(attrs))

			pubRecords := mockProvider.meter.getCounter("rabbitmq.stream.published").getRecords()
			Expect(pubRecords[0].attributes).To(Equal(attrs))

			confRecords := mockProvider.meter.getCounter("rabbitmq.stream.confirmed").getRecords()
			Expect(confRecords[0].attributes).To(Equal(attrs))

			consRecords := mockProvider.meter.getCounter("rabbitmq.stream.consumed").getRecords()
			Expect(consRecords[0].attributes).To(Equal(attrs))

			chunkRecords := mockProvider.meter.getHistogram("rabbitmq.stream.chunk_size").getRecords()
			Expect(chunkRecords[0].attributes).To(Equal(attrs))
		})
	})
})
