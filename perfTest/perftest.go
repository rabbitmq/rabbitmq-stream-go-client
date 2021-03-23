package main

import (
	"bufio"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/gsantomaggio/go-stream-client/pkg/streaming"
	"os"
	"sync"
	"time"
)

func main() {
	fmt.Println("PerfTest")
	fmt.Println("Connecting to RabbitMQ streaming ...")
	/// Constants
	numberOfMessages := 1_000
	batchSize := 100
	numberOfProducers := 3
	numberOfConsumers := 3
	numberOfStreams := 3
	uris := "rabbitmq-streaming://guest:guest@localhost:5551/%2f"
	///

	client, err := streaming.NewClientCreator().Uri(uris).Connect() // create Client Struct
	streaming.CheckErr(err)
	fmt.Printf("Connected to %s \n", uris)

	var producers []*streaming.Producer
	var consumers []*streaming.Consumer
	for i := 0; i < numberOfStreams; i++ {
		streamName := fmt.Sprintf("golang-streaming-%d", i)
		err = client.StreamCreator().Stream(streamName).Create()
		streaming.CheckErr(err)
		for p := 0; p < numberOfProducers; p++ {
			producer, err := client.ProducerCreator().Stream(streamName).Build()
			streaming.CheckErr(err)
			producers = append(producers, producer)
		}

		for p := 0; p < numberOfConsumers; p++ {

			consumer, err := client.ConsumerCreator().
				Stream(streamName).
				Name("my_consumer").
				MessagesHandler(func(consumerId uint8, message *amqp.Message) {

				}).Build()

			streaming.CheckErr(err)
			consumers = append(consumers, consumer)
		}

	}
	// Create AMQP 1.0 messages, see:https://github.com/Azure/go-amqp
	// message aggregation

	start := time.Now()
	var arr []*amqp.Message
	for f := 0; f < batchSize; f++ {
		arr = append(arr, amqp.NewMessage([]byte(fmt.Sprintf("test_%d_%d", 1, f) )))
	}
	time.Sleep(20 * time.Millisecond)
	wg := sync.WaitGroup{}
	for _, producer := range producers {
		wg.Add(1)
		go func(prod *streaming.Producer, wg *sync.WaitGroup) {
			for m := 0; m < numberOfMessages; m++ {
				_, err = prod.BatchPublish(nil, arr) // batch send
				streaming.CheckErr(err)
			}
			wg.Done()
		}(producer, &wg)
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d messages, published in: %s\n", numberOfMessages*numberOfProducers*batchSize*numberOfStreams, elapsed)

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	for _, producer := range producers {
		err = producer.Close()
		streaming.CheckErr(err)
	}
	for _, consumer := range consumers {
		err = consumer.UnSubscribe()
		streaming.CheckErr(err)
	}

	for i := 0; i < numberOfStreams; i++ {
		streamName := fmt.Sprintf("golang-streaming-%d", i)
		err = client.DeleteStream(streamName)
		streaming.CheckErr(err)
	}

	err = client.Close()
	streaming.CheckErr(err)
	fmt.Println("Bye bye")
}
