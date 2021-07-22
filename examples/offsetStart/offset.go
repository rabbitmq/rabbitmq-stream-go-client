package main

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Start Offset example")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest").
			SetMaxConsumersPerClient(1))
	CheckErr(err)
	streamName := uuid.New().String()
	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)

	CheckErr(err)

	producer, err := env.NewProducer(streamName, nil)
	CheckErr(err)

	go func() {
		for i := 0; i < 200; i++ {
			err := producer.Send(amqp.NewMessage([]byte("hello_world_" + strconv.Itoa(i))))
			CheckErr(err)
		}
	}()

	var counter int32
	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		fmt.Printf("messages consumed: %d \n ", atomic.AddInt32(&counter, 1))
	}

	consumerOffsetNumber, err := env.NewConsumer(streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetConsumerName("my_consumer"). // set a consumerOffsetNumber name
			SetOffset(stream.OffsetSpecification{}.Offset(100)))
	// start specific offset, in this case we start from the 100 so it will consume 100 messages
	// see the others stream.OffsetSpecification{}.XXX
	CheckErr(err)

	/// wait a bit just for demo and reset the counters
	time.Sleep(2 * time.Second)
	atomic.StoreInt32(&counter, 0)

	consumerNext, err := env.NewConsumer(streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetConsumerName("my_consumer_1").                // set a consumerOffsetNumber name
			SetOffset(stream.OffsetSpecification{}.First())) // with first() the the stream is loaded from the beginning
	CheckErr(err)

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	err = producer.Close()
	CheckErr(err)
	err = consumerOffsetNumber.Close()
	CheckErr(err)
	err = consumerNext.Close()
	CheckErr(err)
	err = env.DeleteStream(streamName)
	CheckErr(err)

}
