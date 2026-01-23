package main

import (
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

func main() {
	// Create a resource that represents our application or service
	resource, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName("rabbitmq-stream-go-client-metrics-example"),
		semconv.ServiceVersion("0.1.0"),
	))
	if err != nil {
		log.Fatalf("Failed to create resource: %v", err)
	}

	// Create a metric exporter that prints to console
	exporter, err := stdoutmetric.New()
	if err != nil {
		log.Fatalf("Failed to create metric exporter: %v", err)
	}

	// Create a meter provider that uses the resource and exporter
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(resource),
		metric.WithReader(metric.NewPeriodicReader(exporter,
			// Default is 1m. Set to 3s for demonstrative purposes.
			metric.WithInterval(3*time.Second)),
		),
	)

	// Register the meter provider with the global otel.MeterProvider
	// This is a good practice for libraries that use the global meter provider
	otel.SetMeterProvider(meterProvider)

	// Create a new client
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().SetMeterProvider(meterProvider))
	if err != nil {
		log.Fatalf("Failed to create stream environment: %v", err)
	}
	defer env.Close()

	// Create a new stream
	streamName := "test-stream"
	err = env.DeclareStream(streamName, stream.NewStreamOptions())
	if err != nil {
		log.Fatalf("Failed to declare stream: %v", err)
	}
	defer env.DeleteStream(streamName)

	// Create a new producer
	producer, err := env.NewProducer(streamName, stream.NewProducerOptions())
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	const messageCount = 10_000_000
	go func() {
		defer wg.Done()
		for i := 0; i < messageCount; i++ {
			err := producer.Send(amqp.NewMessage([]byte("hello_world_" + strconv.Itoa(i))))
			if err != nil {
				log.Fatalf("Failed to send message: %v", err)
			}
		}
	}()

	consumer, err := env.NewConsumer(streamName,
		messageHandler, stream.NewConsumerOptions().
			SetConsumerName("metrics-consumer").
			SetOffset(stream.OffsetSpecification{}.First()),
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			receivedMessagesMutex.Lock()
			if receivedMessages >= messageCount {
				receivedMessagesMutex.Unlock()
				break
			}
			log.Printf("Received %d of %d messages\n", receivedMessages, messageCount)
			receivedMessagesMutex.Unlock()
			time.Sleep(1 * time.Second)
		}
	}()

	wg.Wait()

	log.Println("Received all messages. Sleeping 3 seconds to let the metrics be printed")
	time.Sleep(3 * time.Second)
}

var receivedMessages int
var receivedMessagesMutex sync.Mutex

func messageHandler(_ stream.ConsumerContext, _ *amqp.Message) {
	receivedMessagesMutex.Lock()
	receivedMessages += 1
	receivedMessagesMutex.Unlock()
}
