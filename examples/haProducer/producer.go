package main

// The ha producer provides a way to auto-reconnect in case of connection problems
// the function handlePublishConfirm is mandatory
// in case of problems the messages have the message.Confirmed == false

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/examples/haProducer/http"
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

var counter int32 = 0
var fail int32 = 0

func handlePublishConfirm(messageStatus []*stream.ConfirmationStatus) {
	go func() {
		for _, message := range messageStatus {
			if message.IsConfirmed() {

				if atomic.AddInt32(&counter, 1)%20000 == 0 {
					fmt.Printf("Confirmed %d messages\n", atomic.LoadInt32(&counter))
				}
			} else {
				if atomic.AddInt32(&fail, 1)%20000 == 0 {
					fmt.Printf("NOT Confirmed %d messages\n", atomic.LoadInt32(&fail))
				}
			}

		}
	}()
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("HA producer example")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	addresses := []string{
		"rabbitmq-stream://guest:guest@localhost:5552/%2f",
		"rabbitmq-stream://guest:guest@localhost:5552/%2f",
		"rabbitmq-stream://guest:guest@localhost:5552/%2f"}

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetUris(addresses))
	CheckErr(err)

	streamName := uuid.New().String()
	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)

	rProducer, err := ha.NewHAProducer(env, streamName, nil, handlePublishConfirm)
	CheckErr(err)
	rProducer1, err := ha.NewHAProducer(env, streamName, nil, handlePublishConfirm)
	CheckErr(err)

	wg := sync.WaitGroup{}

	var sent int32
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			for i := 0; i < 100000; i++ {
				msg := amqp.NewMessage([]byte("ha"))
				err := rProducer.Send(msg)
				CheckErr(err)
				err = rProducer1.BatchSend([]message.StreamMessage{msg})
				if atomic.AddInt32(&sent, 2)%20000 == 0 {
					time.Sleep(100 * time.Millisecond)
					fmt.Printf("Sent..%d messages\n", atomic.LoadInt32(&sent))
				}
				if err != nil {
					break
				}
			}
			wg.Done()
		}(&wg)
	}
	isActive := true
	go func() {
		for isActive {
			coo, err := http.Connections("15672")
			if err != nil {
				return
			}

			for _, connection := range coo {
				_ = http.DropConnection(connection.Name, "15672")
			}
			time.Sleep(2 * time.Second)
		}
	}()

	wg.Wait()
	isActive = false
	time.Sleep(2 * time.Second)

	fmt.Println("Terminated. Press any key to see the report. ")
	_, _ = reader.ReadString('\n')
	time.Sleep(200 * time.Millisecond)
	totalHandled := atomic.LoadInt32(&counter) + atomic.LoadInt32(&fail)
	fmt.Printf("[Report]\n - Sent:%d \n - Confirmed:%d\n - Not confirmed:%d\n - Total messages handeld:%d \n",
		sent, counter, fail, totalHandled)
	if sent == totalHandled {
		fmt.Printf(" - Messages sent %d match with handled: %d! yea! \n\n", sent, totalHandled)
	}

	if totalHandled > sent {
		fmt.Printf(" - Messages sent %d are lower than handled: %d! some duplication, can happens ! \n\n", sent, totalHandled)
	}

	if sent > totalHandled {
		fmt.Printf(" - Messages handled %d are lower than send: %d! that's not good!\n\n", totalHandled, sent)
	}

	err = rProducer.Close()
	CheckErr(err)
	err = rProducer1.Close()
	CheckErr(err)
	err = env.DeleteStream(streamName)
	CheckErr(err)
	err = env.Close()
	CheckErr(err)
}
