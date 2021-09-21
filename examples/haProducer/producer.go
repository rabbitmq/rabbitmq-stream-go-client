package main

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/examples/haProducer/http"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"os"
	"strconv"
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
		for range publishError {
			atomic.AddInt32(&totalMessagesPubError, 1)
			//var data [][]byte
			//if pError.UnConfirmedMessage != nil {
			//	data = pError.UnConfirmedMessage.subEntryMessage.GetData()
			//}
			//fmt.Printf("Error during publish, message:%s ,  error: %s. Total %d  \n", data, pError.Err, totalMessagesPubError)
		}
	}()

}

var counter int32 = 0
var fail int32 = 0

var mut = sync.Mutex{}
var check []string

func findElementInCheck(id string) bool {
	for i, message := range check {
		if message == id {
			check = append(check[:i], check[i+1:]...)
			return true
		}
	}
	return false

}

//func handlePublishConfirm(messagesIds []*stream.UnConfirmedMessage) {
func handlePublishConfirm(confirms stream.ChannelPublishConfirm) {
	go func() {
		for confirmed := range confirms {
			for _, m := range confirmed {
				mut.Lock()
				check = append(check, fmt.Sprintf("%s", m.Message.GetData()[0]))
				mut.Unlock()
				//if m.ProducerID == 254 {
				//	logs.LogError("aaaa")
				//}
				if m.Confirmed {
					if atomic.AddInt32(&counter, 1)%10000 == 0 {
						fmt.Printf("Confirmed %d messages %s\n", atomic.LoadInt32(&counter), m.Message.GetData())
					}
				} else {
					if atomic.AddInt32(&fail, 1)%10000 == 0 {
						fmt.Printf("NOT Confirmed %d messages %s \n", atomic.LoadInt32(&fail), m.Message.GetData())
					}
				}
			}
		}

	}()
}

func consumerClose(channelClose stream.ChannelClose) {
	go func() {
		for event := range channelClose {
			fmt.Printf("producer: %s closed on the stream: %s, reason: %s \n", event.Name, event.StreamName, event.Reason)
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
	rProducer, err := env.NewProducer(streamName, nil)
	rProducer.NotifyPublishConfirmation()
	handlePublishConfirm(rProducer.NotifyPublishConfirmation())
	consumerClose(rProducer.NotifyClose())
	//rProducer, err := ha.NewHAProducer(env, streamName, stream.NewProducerOptions().
	//	SetBatchPublishingDelay(500), handlePublishConfirm)
	//CheckErr(err)
	//rProducer1, err := ha.NewHAProducer(env, streamName, nil, handlePublishConfirm)
	//CheckErr(err)
	var bodyElement int32
	wg := sync.WaitGroup{}
	elementsToSend := 80000
	var sent int32
	for z := 0; z < 1; z++ {
		wg.Add(1)
		go func(wg *sync.WaitGroup, idx int) {

			for i := 1; i <= elementsToSend; i++ {
				msg := amqp.NewMessage([]byte("" + strconv.Itoa(int(atomic.AddInt32(&bodyElement, 1)))))
				err := rProducer.Send(msg)
				if atomic.AddInt32(&sent, 1)%1000 == 0 {
					time.Sleep(100 * time.Millisecond)
					fmt.Printf("Sent..%d messages\n", atomic.LoadInt32(&sent))
				}
				//time.Sleep(300 * time.Millisecond)
				//CheckErr(err)
				//err = rProducer1.Send(msg)
				//CheckErr(err)
				if err != nil {
					time.Sleep(500 * time.Millisecond)
					break
				}
			}
			//time.Sleep(100 * time.Millisecond)
			wg.Done()
		}(&wg, z)
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

			time.Sleep(2 * time.Second)
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

	err = rProducer.Close()
	//CheckErr(err)
	//err = rProducer1.Close()
	//CheckErr(err)

	time.Sleep(200 * time.Millisecond)
	fmt.Printf("[Report]\nSent:%d \nConfirmed:%d\nNot confirmed:%d\nPublish error:%d\nTotal messages handeld:%d \n",
		sent, counter, fail, totalMessagesPubError, atomic.LoadInt32(&counter)+atomic.LoadInt32(&fail)+
			atomic.LoadInt32(&totalMessagesPubError))

	logs.LogInfo("checking elements len: %d", len(check))

	//for i := 1; i <= int(bodyElement); i++ {
	//	if i%100 == 0 {
	//		logs.LogInfo("checking %d", i)
	//	}
	//	if findElementInCheck(strconv.Itoa(i)) == false {
	//		logs.LogError("no")
	//
	//	}
	//
	//}

	err = consumer.Close()
	CheckErr(err)
	err = env.DeleteStream(streamName)
	CheckErr(err)
	err = env.Close()
	CheckErr(err)

}
