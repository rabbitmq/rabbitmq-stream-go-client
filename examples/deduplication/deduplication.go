package main

import (
	"bufio"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"os"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Deduplication example")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("localhost").
			SetPort(5552))
	CheckErr(err)
	streamName := "deduplication"
	err = env.DeclareStream(streamName,
		stream.NewStreamOptions().SetMaxLengthBytes(stream.ByteCapacity{}.GB(2)))
	if err != stream.StreamAlreadyExists {
		CheckErr(err)
	}

	producer, err := env.NewProducer(streamName,
		stream.NewProducerOptions().SetProducerName("myProducer")) // producer name is mandatory to handle the deduplication

	CheckErr(err)

	data := make(map[int64]string)
	data[1] = "Ferrari"
	data[2] = "Ducati"
	data[3] = "Maserati"
	data[4] = "Fiat"
	data[5] = "Lamborghini"
	data[6] = "Bugatti"
	data[7] = "Alfa Romeo"
	data[8] = "Aprilia"
	data[9] = "Benelli"
	data[10] = "Piaggio"

	for i := 0; i < len(data); i++ {
		var msg message.StreamMessage
		msg = amqp.NewMessage([]byte(data[int64(i)]))
		msg.SetPublishingId(int64(i)) // mandatory to handle the deduplication
		err := producer.Send(msg)
		CheckErr(err)
	}

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	err = producer.Close()
	CheckErr(err)
}
