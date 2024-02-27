package main

// The ha producer provides a way to auto-reconnect in case of connection problems
// the function handlePublishConfirm is mandatory
// in case of problems the messages have the message.Confirmed == false

import (
	"bufio"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
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

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("HA producer example")
	fmt.Println("Connecting to RabbitMQ streaming ...")
	const messagesToSend = 500_000
	const numberOfProducers = 7

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
	var sent int32
	isRunning := true
	go func() {
		for isRunning {
			totalHandled := atomic.LoadInt32(&confirmed) + atomic.LoadInt32(&fail)
			fmt.Printf("%s - ToSend: %d - Sent:%d  - Confirmed:%d  - Not confirmed:%d - Total :%d \n",
				time.Now().Format(time.RFC822), messagesToSend*numberOfProducers, sent, confirmed, fail, totalHandled)
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
					atomic.AddInt32(&sent, 1)
					CheckErr(err)
				}
				unConfirmedMessages = []message.StreamMessage{}
				mutex.Unlock()
				err := rProducer.Send(msg)
				time.Sleep(1 * time.Millisecond)
				atomic.AddInt32(&sent, 1)
				CheckErr(err)
			}
		}()

	}

	fmt.Println("Terminated. Press enter to close the connections.")
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
