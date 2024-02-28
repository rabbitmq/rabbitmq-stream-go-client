package main

// The ha producer provides a way to auto-reconnect in case of connection problems
// the function handlePublishConfirm is mandatory
// in case of problems the messages have the message.Confirmed == false

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

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
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("HA producer example")
	fmt.Println("Connecting to RabbitMQ streaming ...")
	const messagesToSend = 1_500_000
	const numberOfProducers = 3
	const numberOfConsumers = 3
	const sendDelay = 10 * time.Millisecond

	addresses := []string{
		//"rabbitmq-stream://guest:guest@node1:5572/%2f",
		//"rabbitmq-stream://guest:guest@node1:5572/%2f",
		"rabbitmq-stream://guest:guest@localhost:5552/%2f"}

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetMaxProducersPerClient(4).
			SetUris(addresses))
	CheckErr(err)

	streamName := "golang-reliable-producer-Test"
	env.DeleteStream(streamName)

	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)

	isRunning := true
	go func() {
		for isRunning {
			totalHandled := atomic.LoadInt32(&confirmed) + atomic.LoadInt32(&fail)
			fmt.Printf("%s - ToSend: %d - nProducers: %d - nConsumers %d \n", time.Now().Format(time.RFC822),
				messagesToSend*numberOfProducers, numberOfProducers, numberOfConsumers)
			fmt.Printf("Sent:%d - ReSent %d - Confirmed:%d  - Not confirmed:%d - Total :%d \n",
				sent, reSent, confirmed, fail, totalHandled)
			fmt.Printf("Total Consumed: %d - Per consumer: %d  \n", consumed, consumed/numberOfConsumers)
			fmt.Printf("********************************************\n")

			time.Sleep(5 * time.Second)
		}
	}()
	var producers []*ha.ReliableProducer
	for i := 0; i < numberOfProducers; i++ {
		var mutex = sync.Mutex{}
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
				time.Sleep(sendDelay)
				atomic.AddInt32(&sent, 1)
				CheckErr(err)
			}
		}()
	}
	for i := 0; i < numberOfConsumers; i++ {
		go func(name string) {
			isActive := true
			offset := stream.OffsetSpecification{}.First()
			for isActive {
				fmt.Printf("Creating consumer for stream: %s \n", name)
				consumer, err := newConsumer(env, name, func(consumerContext stream.ConsumerContext, message *amqp.Message) {
					offset = stream.OffsetSpecification{}.Offset(consumerContext.Consumer.GetOffset() + 1)
					atomic.AddInt32(&consumed, 1)
				}, stream.NewConsumerOptions().
					SetConsumerName("my_consumer").
					SetOffset(offset))
				if errors.Is(err, stream.StreamNotAvailable) {
					sleepValue := rand.Intn(int((5-2+1)+2)*1000) + 2*1000
					fmt.Printf("Stream not available: %s reconnecting in %d milliseconds \n", name, sleepValue)
					time.Sleep(time.Duration(sleepValue) * time.Millisecond)
					continue
				}
				event := consumer.NotifyClose()
				a := <-event
				fmt.Printf("Consumer: %s closed on the stream: %s, reason: %s \n", a.Name, a.StreamName, a.Reason)
				time.Sleep(1 * time.Second)
				isActive = a.Reason == "socket client closed"
			}

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
	isRunning = false
	err = env.Close()
	CheckErr(err)

}

func newConsumer(env *stream.Environment, streamName string, handleMessages func(consumerContext stream.ConsumerContext, message *amqp.Message), consumerConfig *stream.ConsumerOptions) (*stream.Consumer, error) {
	consumer, err := env.NewConsumer(
		streamName,
		handleMessages,
		consumerConfig,
	)
	return consumer, err
}
