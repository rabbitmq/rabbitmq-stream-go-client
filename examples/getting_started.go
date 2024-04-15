package main

import (
	"bufio"
	"fmt"
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

func handlePublishConfirm(confirms stream.ChannelPublishConfirm) {
	go func() {
		for confirmed := range confirms {
			for _, msg := range confirmed {
				if msg.IsConfirmed() {
					fmt.Printf("message %s stored \n  ", msg.GetMessage().GetData())
				} else {
					fmt.Printf("message %s failed \n  ", msg.GetMessage().GetData())
				}

			}
		}
	}()
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
			SetWriteBuffer(1024 * 1024).
			SetReadBuffer(1024 * 1024).
			SetHost("localhost").
			SetPort(5552).
			SetUser("guest").
			SetPassword("guest"))
	CheckErr(err)

	//producer, err := env.NewProducer("inc", stream.NewProducerOptions())
	//if err != nil {
	//	return
	//}

	//for i := 0; i < 200_000_000; i++ {
	//	err = producer.Send(amqp.NewMessage([]byte(fmt.Sprintf("%d", i))))
	//	CheckErr(err)
	//
	//	if i%500000 == 0 {
	//		fmt.Printf("Sent %d messages\n", i)
	//	}
	//}

	start := time.Now()
	startBeginning := time.Now()
	var recv int32
	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {

		id := fmt.Sprintf("%s", message.GetData())
		i, err := strconv.Atoi(id)
		if err != nil {
			fmt.Println(err)
		}
		if recv != int32(i) {
			fmt.Printf("Expected %d, got %d\n", recv, i)
		}

		if atomic.AddInt32(&recv, 1)%5000000 == 0 {
			fmt.Printf("[ %d  messages] - time elapsed: %s - from the begninning: %s\n", recv,
				time.Since(start), time.Since(startBeginning))
			start = time.Now()
		}

	}

	//chMessage := make(chan []*amqp.Message, 10)
	//
	//go func() {
	//	for msg := range chMessage {
	//		for _, _ = range msg {
	//			if atomic.AddInt32(&recv, 1)%5000000 == 0 {
	//				fmt.Printf("Time elapsed: %d %s\n", recv, time.Since(start))
	//				start = time.Now()
	//			}
	//		}
	//	}
	//
	//}()

	consumer, err := env.NewConsumer(
		"inc",
		handleMessages,
		stream.NewConsumerOptions().
			SetInitialCredits(100).
			//SetChMessage(chMessage).
			SetClientProvidedName("my_consumer").            // connection name
			SetConsumerName("my_consumer").                  // set a consumer name
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
	CheckErr(err)
	err = env.Close()
	CheckErr(err)
}
