package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
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
		stream.NewProducerOptions().
			// producer name is mandatory to handle the deduplication
			// don't use the producer name if you don't need the deduplication
			SetProducerName("myProducer"))

	CheckErr(err)

	chConfirm := producer.NotifyPublishConfirmation()
	go func(ch stream.ChannelPublishConfirm, _ *stream.Producer) {
		for messagesStatus := range ch {
			for _, messageStatus := range messagesStatus {
				if messageStatus.IsConfirmed() {
					fmt.Printf("publishingId: %d - Confirmed: %s \n",
						/// In this case the PublishingId is the one provided by the user
						messageStatus.GetMessage().GetPublishingId(),
						messageStatus.GetMessage().GetData()[0])
				}
			}
		}
	}(chConfirm, producer)

	// In case you need to know which is the last ID for the producer: GetLastPublishingId
	lastPublishingId, err := producer.GetLastPublishingId()
	CheckErr(err)
	fmt.Printf("lastPublishingId: %d\n",
		lastPublishingId,
	)

	data := make(map[int]string)
	data[0] = "Piaggio"
	data[1] = "Ferrari"
	data[2] = "Ducati"
	data[3] = "Maserati"
	data[4] = "Fiat"
	data[5] = "Lamborghini"
	data[6] = "Bugatti"
	data[7] = "Alfa Romeo"
	data[8] = "Aprilia"
	data[9] = "Benelli"

	for i := range len(data) {
		msg := amqp.NewMessage([]byte(data[i]))
		msg.SetPublishingId(int64(i)) // mandatory to handle the deduplication
		err := producer.Send(msg)
		CheckErr(err)
	}

	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')
	err = producer.Close()
	CheckErr(err)
	err = env.DeleteStream(streamName)
	CheckErr(err)
}
