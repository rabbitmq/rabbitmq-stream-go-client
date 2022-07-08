package main

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"os"
	"strconv"
	"time"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}

func consumerClose(channelClose stream.ChannelClose) {
	event := <-channelClose
	fmt.Printf("Consumer: %s closed on the stream: %s, reason: %s \n", event.Name, event.StreamName, event.Reason)
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	// Set log level, not mandatory by default is INFO
	// you cn set DEBUG for more information
	// stream.SetLevelInfo(logs.DEBUG)

	fmt.Println("Getting started with Streaming client for RabbitMQ")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	// Connect to the broker ( or brokers )
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
	CheckErr(err)
	// Create a stream, you can create streams without any option like:
	// err = env.DeclareStream(streamName, nil)
	// it is a best practise to define a size,  1GB for example:

	streamName := uuid.New().String()
	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)

	CheckErr(err)

	// Get a new producer for a stream
	producer, err := env.NewProducer(streamName, nil)
	mychannel := producer.NotifyPublishConfirmation()
	CheckErr(err)

	go func(v chan stream.ProducerResponse) {
		for r := range mychannel {
			fmt.Printf("Confirmation id: %d\n", r.GetListOfConfirmations())

		}
	}(mychannel)

	for i := 0; i < 100; i++ {
		err := producer.Send(amqp.NewMessage([]byte("hello_world_" + strconv.Itoa(i))))
		time.Sleep(10 * time.Millisecond)
		CheckErr(err)
	}

	/*
		mychannel := new(chan ProducerSocketData)
		producer.SetDataChannel(mychannel)
		go func() {
			rawData := <- mychannel
			if rawData.command == confirms {
				listOfConfirms, err := rawData.Parse()
				if err != nil ...
			}
			if rawData.command == publishError {
				err := rawData.Parse()
				if err != nil ...
		}



	*/

	////optional publish confirmation channel
	//chPublishConfirm := producer.NotifyPublishConfirmation()
	//handlePublishConfirm(chPublishConfirm)

	// the send method automatically aggregates the messages
	// based on batch size
	for i := 0; i < 1000; i++ {
		err := producer.Send(amqp.NewMessage([]byte("hello_world_" + strconv.Itoa(i))))
		CheckErr(err)
	}

	// this sleep is not mandatory, just to show the confirmed messages
	time.Sleep(1 * time.Second)
	err = producer.Close()
	CheckErr(err)

	// Define a consumer per stream, there are different offset options to define a consumer, default is
	//env.NewConsumer(streamName, func(Context streaming.ConsumerContext, message *amqp.message) {
	//
	//}, nil)
	// if you need to track the offset you need a consumer name like:
	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		fmt.Printf("consumer name: %s, text: %s \n ", consumerContext.Consumer.GetName(), message.Data)
	}

	consumer, err := env.NewConsumer(
		streamName,
		handleMessages,
		stream.NewConsumerOptions().
			SetConsumerName("my_consumer").
			SetOffset(stream.OffsetSpecification{}.First()). // start consuming from the beginning
			SetCRCCheck(false))                              // Disable crc control, increase the performances
	CheckErr(err)
	channelClose := consumer.NotifyClose()

	// channelClose receives all the closing events, here you can handle the
	// client reconnection or just log
	defer consumerClose(channelClose)

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	err = consumer.Close()
	time.Sleep(200 * time.Millisecond)
	CheckErr(err)
	err = env.DeleteStream(streamName)
	CheckErr(err)
	err = env.Close()
	CheckErr(err)
}
