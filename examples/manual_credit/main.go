package main

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func main() {
	done := make(chan struct{})
	reader := bufio.NewReader(os.Stdin)

	l := log.New(os.Stdout, "[manual_credit] ", log.LstdFlags)
	l.Println("Starting Manual credit example")
	l.Println("Connecting to RabbitMQ streaming ...")

	// Connect to the broker ( or brokers )
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
	if err != nil {
		l.Printf("Error creating environment: %s", err)
		panic(err)
	}
	defer env.Close()

	l.Println("Declaring stream")
	const streamName = "manual_credit"
	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)
	if err != nil {
		l.Printf("Error declaring stream: %s", err)
		panic(err)
	}
	defer env.DeleteStream(streamName)

	// Get a new producer for a stream
	l.Println("Creating producer")
	opts := stream.NewProducerOptions().SetBatchSize(10_000)
	producer, err := env.NewProducer(streamName, opts)
	if err != nil {
		l.Printf("Error creating producer: %s", err)
		panic(err)
	}

	// the send method automatically aggregates the messages
	// based on batch size
	l.Println("Sending 1,000,000 messages")
	for i := range 1_000_000 {
		err := producer.Send(amqp.NewMessage([]byte("hello_world_" + strconv.Itoa(i))))
		if err != nil {
			l.Printf("Error sending message: %s", err)
			panic(err)
		}
	}

	time.Sleep(1 * time.Second)
	l.Println("Messages sent, closing producer")
	err = producer.Close()
	if err != nil {
		l.Printf("Error closing producer: %s", err)
		panic(err)
	}

	count := 0
	receivedBatches := 0
	lastBatchSize := -1
	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		select {
		case <-done:
			return
		default:
		}
		if lastBatchSize == -1 {
			lastBatchSize = int(consumerContext.GetEntriesCount())
		}
		count++
		if count%10_000 == 0 {
			l.Printf("Received %d messages\n", count)
		}
		if count%lastBatchSize == 0 {
			receivedBatches++
			lastBatchSize = -1
		}
		if receivedBatches == 10 {
			l.Printf("Received %d chunks, sending 10 credits\n", receivedBatches)
			err := consumerContext.Consumer.Credit(10)
			if err != nil {
				l.Printf("Error sending credits: %s\n", err)
			}
			receivedBatches = 0
		}
	}

	l.Println("Creating consumer")
	consumer, err := env.NewConsumer(
		streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetClientProvidedName("my_consumer"). // connection name
			SetConsumerName("my_consumer").       // set a consumer name
			SetOffset(stream.OffsetSpecification{}.First()).
			SetCRCCheck(false).
			SetCreditStrategy(stream.ManualCreditStrategy).
			SetInitialCredits(10)) // Initial credit
	if err != nil {
		panic(err)
	}

	l.Println("Press any key to stop ")
	_, _, _ = reader.ReadLine()
	close(done)
	time.Sleep(200 * time.Millisecond)
	l.Printf("Exited after consuming %d messages\n", count)
	err = consumer.Close()
	if err != nil {
		l.Printf("Error closing consumer: %s\n", err)
	}
}
