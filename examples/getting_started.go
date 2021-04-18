package main

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/streaming"
	"os"
	"sync/atomic"
	"time"
)

func CheckErr(err error) {
	if err != nil {
		streaming.ERROR("%s ", err)
	}
	return
}
func main() {
	reader := bufio.NewReader(os.Stdin)
	streaming.INFO("Getting started with Streaming client for RabbitMQ")
	streaming.INFO("Connecting to RabbitMQ streaming ...")
	uris := "rabbitmq-streaming://guest:guest@localhost:5551/%2f"
	client, err := streaming.NewClientCreator().
		Uri(uris).
		Connect() // Create Client
	CheckErr(err)
	if err != nil {
		return
	}

	streaming.INFO("Connected to: %s", uris)
	streamName := uuid.New().String()
	err = client.StreamCreator().Stream(streamName).
		Create() // Create the streaming queue
	CheckErr(err)

	err = client.StreamCreator().Stream(streamName).
		MaxLengthBytes(streaming.ByteCapacity{}.MB(5)).
		Create() // Create the streaming queue
	CheckErr(err)

	var count int32
	consumer, err := client.ConsumerCreator().
		Stream(streamName).
		Name(uuid.NewString()).
		MessagesHandler(func(context streaming.ConsumerContext, message *amqp.Message) {
			streaming.INFO("Message number:%d consumer id:%d data:%s \n",
				atomic.AddInt32(&count, 1), context.Consumer.ID,
				message.Data)
			err := context.Consumer.Commit()
			CheckErr(err)
		}).Build()
	CheckErr(err)

	// Get a new producer to publish the messages
	clientProducer, err := streaming.NewClientCreator().Uri(uris).
		PublishErrorHandler(func(publisherId uint8, publishingId int64, code uint16) {
			streaming.ERROR("Publish Error, publisherId %d, code: %s", publisherId, streaming.LookErrorCode(code))
		}).
		Connect()
	CheckErr(err)
	producer, err := clientProducer.ProducerCreator().Stream(streamName).Build()
	CheckErr(err)

	//
	numberOfSend := 10
	batchSize := 10

	// Create AMQP 1.0 messages, see:https://github.com/Azure/go-amqp
	// message aggregation
	countM := 0
	start := time.Now()
	for z := 0; z < numberOfSend; z++ {
		var arr []*amqp.Message
		for f := 0; f < batchSize; f++ {
			countM++
			arr = append(arr, amqp.NewMessage([]byte(fmt.Sprintf("test_%d", countM) )))
		}
		_, err = producer.BatchPublish(nil, arr) // batch send
	}

	elapsed := time.Since(start)
	streaming.INFO("%d messages, published in: %s\n", numberOfSend*batchSize, elapsed)

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	err = producer.Close()
	CheckErr(err)
	err = consumer.UnSubscribe()
	CheckErr(err)
	err = client.DeleteStream(streamName) // Remove the streaming queue and the data
	CheckErr(err)
	err = client.Close()
	CheckErr(err)
	fmt.Println("Bye bye")
}
