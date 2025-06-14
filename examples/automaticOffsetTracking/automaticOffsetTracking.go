package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Automatic Offset tracking example")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
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
		for i := range 220 {
			err := producer.Send(amqp.NewMessage([]byte("hello_world_" + strconv.Itoa(i))))
			CheckErr(err)
		}
	}()

	var counter int32
	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		if atomic.AddInt32(&counter, 1)%20 == 0 {
			fmt.Printf("messages consumed with auto commit: %d \n ", atomic.LoadInt32(&counter))
		}
	}

	consumerOffsetNumber, err := env.NewConsumer(streamName,
		handleMessages,
		stream.NewConsumerOptions().
			// set a consumerOffsetNumber name
			SetConsumerName("my_consumer").
			// nil is also a valid value. Default values will be used
			SetAutoCommit(stream.NewAutoCommitStrategy().
										SetCountBeforeStorage(50). // each 50 messages stores the index
										SetFlushInterval(20*time.Second)).
			SetOffset(stream.OffsetSpecification{}.First())) // or after 20 seconds
	CheckErr(err)

	time.Sleep(2 * time.Second)
	atomic.StoreInt32(&counter, 0)
	// so here we consume only 20 messages
	handleMessagesAfter := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		if atomic.AddInt32(&counter, 1)%20 == 0 {
			fmt.Printf("messages consumed after: %d \n ", atomic.LoadInt32(&counter))
		}
	}

	offset, err := env.QueryOffset("my_consumer", streamName)
	CheckErr(err)
	consumerNext, err := env.NewConsumer(streamName,
		handleMessagesAfter,
		stream.NewConsumerOptions().
			SetConsumerName("my_consumer").                         // set a consumerOffsetNumber name
			SetOffset(stream.OffsetSpecification{}.Offset(offset))) // With last consumed we point to the last saved.
	// in this case will be 200. So it will consume 20
	// messages
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
