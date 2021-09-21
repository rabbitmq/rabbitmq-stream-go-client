package main

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/examples/haProducer/http"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
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

var totalMessagesPubError int32

func handlePublishError(publishError stream.ChannelPublishError) {
	go func() {
		for {
			<-publishError
			atomic.AddInt32(&totalMessagesPubError, 1)
			//var data [][]byte
			//if pError.UnConfirmedMessage != nil {
			//	data = pError.UnConfirmedMessage.Message.GetData()
			//}
			////fmt.Printf("Error during publish, message:%s ,  error: %s. Total %d  \n", data, pError.Err, totalMessagesPubError)
		}
	}()

}

var counter int32 = 0
var fail int32 = 0

func handlePublishConfirm(confirms stream.ChannelPublishConfirm) {
	go func() {
		for messagesIds := range confirms {
			for _, m := range messagesIds {
				if m.Confirmed {
					if atomic.AddInt32(&counter, 1)%5000 == 0 {
						fmt.Printf("Confirmed %d messages\n", atomic.LoadInt32(&counter))
					}
				} else {
					if atomic.AddInt32(&fail, 1)%1000 == 0 {
						fmt.Printf("NOT Confirmed %d messages\n", atomic.LoadInt32(&fail))
					}
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
		stream.NewEnvironmentOptions().SetUris(addresses))
	CheckErr(err)

	streamName := "test"
	err = env.DeclareStream(streamName,
		&stream.StreamOptions{
			MaxLengthBytes: stream.ByteCapacity{}.GB(2),
		},
	)

	rProducer, err := ha.NewHAProducer(env, streamName, nil)
	CheckErr(err)
	//rProducer1, err := ha.NewHAProducer(env, streamName, nil)
	//CheckErr(err)

	chPublishConfirm := rProducer.NotifyPublishConfirmation()
	handlePublishConfirm(chPublishConfirm)

	chPublishErr := rProducer.NotifyPublishError()
	handlePublishError(chPublishErr)

	//handlePublishConfirm(rProducer1.NotifyPublishConfirmation())
	//
	//handlePublishError(rProducer1.NotifyPublishError())

	wg := sync.WaitGroup{}

	var sent int32
	for i := 0; i < 11; i++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			//mutex.Lock()
			//mutex.Unlock()
			for i := 0; i < 100000; i++ {
				msg := amqp.NewMessage([]byte("ha"))
				err := rProducer.Send(msg)
				//CheckErr(err)
				//err = rProducer1.Send(msg)

				if atomic.AddInt32(&sent, 2)%5000 == 0 {
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
	go func() {

		for {

			coo, err := http.Connections("15672")
			if err != nil {
				return
			}

			for _, connection := range coo {
				http.DropConnection(connection.Name, "15672")
			}

			time.Sleep(3 * time.Second)
		}

	}()

	wg.Wait()

	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {

	}

	consumer, err := env.NewConsumer(streamName,
		handleMessages,
		stream.NewConsumerOptions().SetOffset(stream.OffsetSpecification{}.Last()).
			SetConsumerName(uuid.New().String()))
	CheckErr(err)

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	time.Sleep(200 * time.Millisecond)
	fmt.Printf("[Report]\nSent:%d \nConfirmed:%d\nNot confirmed:%d\nPublish error:%d\nTotal messages handeld:%d \n",
		sent, counter, fail, totalMessagesPubError, atomic.LoadInt32(&counter)+atomic.LoadInt32(&fail)+
			atomic.LoadInt32(&totalMessagesPubError))

	err = rProducer.Close()
	//CheckErr(err)
	//err = rProducer1.Close()
	CheckErr(err)
	err = consumer.Close()
	CheckErr(err)
	err = env.DeleteStream(streamName)
	CheckErr(err)
	err = env.Close()
	CheckErr(err)
}
