package main

import (
	"bufio"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"os"
	"sync/atomic"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	serverUri := os.Args[1]
	streamName := os.Args[2]
	offsetStart := os.Args[3]
	offsetSpec := stream.OffsetSpecification{}.First()

	switch offsetStart {
	case "first":
		offsetSpec = stream.OffsetSpecification{}.First()
	case "last":
		offsetSpec = stream.OffsetSpecification{}.Last()
	case "next":
		offsetSpec = stream.OffsetSpecification{}.Next()

	}

	fmt.Printf("Stream Tail, serverUri: %s, streamName: %s, offsetStart: %s \n",
		serverUri, streamName, offsetStart)
	fmt.Println("Connecting to RabbitMQ streaming ...")

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().SetUri(serverUri).
			SetMaxConsumersPerClient(1))
	CheckErr(err)

	var counter int32
	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		prop := message.Properties
		appProp := message.ApplicationProperties
		fmt.Printf("\n")
		fmt.Printf("message body: %s properties: %s, app properties: %s, consumed: %d \n ",
			message.GetData(), prop, appProp, atomic.AddInt32(&counter, 1))
	}

	consumerTail, err := env.NewConsumer(streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetOffset(offsetSpec))
	CheckErr(err)

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	CheckErr(err)
	err = consumerTail.Close()
	CheckErr(err)
	CheckErr(err)

}
