package main

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/streaming"
	"os"
	"strconv"
)

func CreateArrayMessagesForTesting(numberOfMessages int) []*amqp.Message {
	var arr []*amqp.Message
	for z := 0; z < numberOfMessages; z++ {
		arr = append(arr, amqp.NewMessage([]byte("test_"+strconv.Itoa(z))))
	}
	return arr
}
func main() {
	env, err := streaming.NewEnvironment(
		streaming.NewEnvironmentOptions().
			Uri("rabbitmq-streaming://test:test@localhost:5551/%2f").
			OnPublishError(func(publisherId uint8,
				publishingId int64,
				code uint16,
				errorMessage string) {
				fmt.Printf("Errrrrrrrrrr %d %d %d %s\n", publisherId, publishingId, code, errorMessage)
			}))
	//streaming.NewEnvironmentOptions().Host("34.76.8.103").UserName("replicator").
	//	Password("guest"),
	//)
	if err != nil {
		return
	}

	streamname := uuid.New().String()
	err = env.DeclareStream(streamname, nil)
	for i := 0; i < 1; i++ {

		producer, err := env.NewProducer(streamname,
			streaming.NewProducerOptions().OnPublishConfirm(func(ch <-chan []int64) {
				ids := <-ch
				fmt.Printf("ids: %d", len(ids))
			}))
		if err != nil {
			return
		}

		//go func() {
		//	time.Sleep( 1000 * time.Millisecond)
		//	producer.Close()
		//}()
		go func() {
			for i := 0; i < 100; i++ {

				_, err = producer.BatchPublish(nil, CreateArrayMessagesForTesting(1000))
				//producer.Close()
				//time.Sleep(20 * time.Millisecond)
				if err != nil {
					fmt.Printf("bbb %s \n", err)
					return
				}
				//fmt.Println("aaa")

			}
		}()
		producer.Close()
	}
	//err = env.DeleteStream(streamname)
	//if err != nil {
	//	return
	//}
	//time.Sleep( 50 * time.Millisecond)
	//producer.Close()

	//err = producer.Close()
	//if err != nil {
	//	return
	//}

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')

}
