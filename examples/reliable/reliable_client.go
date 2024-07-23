package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

// The ha producer and consumer provide a way to auto-reconnect in case of connection problems

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}

var confirmed int32 = 0
var fail int32 = 0
var consumed int32 = 0
var sent int32
var reSent int32

func main() {
	// Tune the parameters to test the reliability
	const messagesToSend = 5_000_000
	const numberOfProducers = 2
	const concurrentProducers = 2
	const numberOfConsumers = 2
	const sendDelay = 1 * time.Millisecond
	const delayEachMessages = 200
	const maxProducersPerClient = 4
	const maxConsumersPerClient = 2
	//

	reader := bufio.NewReader(os.Stdin)
	stream.SetLevelInfo(logs.DEBUG)
	fmt.Println("Reliable Producer/Consumer example")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	addresses := []string{
		//"rabbitmq-stream://guest:guest@node1:5572/%2f",
		//"rabbitmq-stream://guest:guest@node1:5572/%2f",
		"rabbitmq-stream://guest:guest@localhost:5552/%2f"}

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetMaxProducersPerClient(maxProducersPerClient).
			SetMaxConsumersPerClient(maxConsumersPerClient).
			SetUris(addresses))
	CheckErr(err)
	fmt.Printf("Environment created with %d producers and %d consumers\n\n", maxProducersPerClient, maxConsumersPerClient)

	streamName := "golang-reliable-Test"

	err = env.DeleteStream(streamName)
	// If the stream does not exist,
	// we don't care here as we are going to create it anyway
	if !errors.Is(err, stream.StreamDoesNotExist) {
		CheckErr(err)
	}
	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)
	CheckErr(err)

	isRunning := true
	go func() {
		for isRunning {
			totalConfirmed := atomic.LoadInt32(&confirmed) + atomic.LoadInt32(&fail)
			expectedMessages := messagesToSend * numberOfProducers * concurrentProducers
			fmt.Printf("%s - ToSend: %d - nProducers: %d - concurrentProducers: %d - nConsumers %d \n", time.Now().Format(time.RFC822),
				expectedMessages, numberOfProducers, concurrentProducers, numberOfConsumers)
			fmt.Printf("Sent:%d - ReSent %d - Confirmed:%d  - Not confirmed:%d - Fail+Confirmed  :%d \n",
				sent, atomic.LoadInt32(&reSent), atomic.LoadInt32(&confirmed), atomic.LoadInt32(&fail), totalConfirmed)
			fmt.Printf("Total Consumed: %d - Per consumer: %d  \n", atomic.LoadInt32(&consumed),
				atomic.LoadInt32(&consumed)/numberOfConsumers)
			fmt.Printf("********************************************\n")
			time.Sleep(5 * time.Second)
		}
	}()
	var producers []*ha.ReliableProducer
	for i := 0; i < numberOfProducers; i++ {
		var mutex = sync.Mutex{}
		// Here we store the messages that have not been confirmed
		// then we resend them.
		// Note: This is only for test. The list can grow indefinitely
		var unConfirmedMessages []message.StreamMessage
		rProducer, err := ha.NewReliableProducer(env,
			streamName,
			stream.NewProducerOptions().
				SetConfirmationTimeOut(5*time.Second).
				SetClientProvidedName(fmt.Sprintf("producer-%d", i)),
			func(messageStatus []*stream.ConfirmationStatus) {
				go func() {
					for _, msgStatus := range messageStatus {
						if msgStatus.IsConfirmed() {
							atomic.AddInt32(&confirmed, 1)
						} else {
							atomic.AddInt32(&fail, 1)
							mutex.Lock()
							unConfirmedMessages = append(unConfirmedMessages, msgStatus.GetMessage())
							mutex.Unlock()
						}
					}
				}()
			})
		CheckErr(err)
		producers = append(producers, rProducer)
		go func() {
			for i := 0; i < concurrentProducers; i++ {
				go func() {
					for i := 0; i < messagesToSend; i++ {
						msg := amqp.NewMessage([]byte("ha"))
						mutex.Lock()
						for _, confirmedMessage := range unConfirmedMessages {
							err := rProducer.Send(confirmedMessage)
							atomic.AddInt32(&reSent, 1)
							CheckErr(err)
						}
						unConfirmedMessages = []message.StreamMessage{}
						mutex.Unlock()
						err := rProducer.Send(msg)
						if i%delayEachMessages == 0 {
							time.Sleep(sendDelay)
						}
						atomic.AddInt32(&sent, 1)
						CheckErr(err)

					}
				}()
			}
		}()
	}
	var consumers []*ha.ReliableConsumer

	for i := 0; i < numberOfConsumers; i++ {
		go func(name string) {
			consumer, err := ha.NewReliableConsumer(env,
				streamName,
				stream.NewConsumerOptions().SetOffset(stream.OffsetSpecification{}.First()),
				func(consumerContext stream.ConsumerContext, message *amqp.Message) {
					atomic.AddInt32(&consumed, 1)
				})
			CheckErr(err)
			consumers = append(consumers, consumer)
		}(streamName)
	}

	fmt.Println("Press enter to close the connections.")
	_, _ = reader.ReadString('\n')
	for _, producer := range producers {
		err := producer.Close()
		if err != nil {
			CheckErr(err)
		}
	}
	for _, consumer := range consumers {
		err := consumer.Close()
		if err != nil {
			CheckErr(err)
		}
	}
	isRunning = false
	fmt.Println("Connections Closed. Press enter to close the environment.")
	_, _ = reader.ReadString('\n')

	err = env.Close()
	CheckErr(err)
}
