package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"os"
	"strings"
	"time"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}
func main() {

	fmt.Println("Super stream example")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	// Connect to the broker ( or brokers )
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions())
	CheckErr(err)
	superStreamName := "MySuperStream"
	err = env.DeleteSuperStream(superStreamName)
	if err != nil && errors.Is(err, stream.StreamDoesNotExist) {
		// we can ignore the error if the stream does not exist
		// it will be created later
		fmt.Printf("error deleting suepr stream: %s", err)
	} else {
		CheckErr(err)
	}

	// Create a super stream
	err = env.DeclareSuperStream(superStreamName, stream.NewPartitionsSuperStreamOptions(3))
	CheckErr(err)

	// Create a producer
	producer, err := env.NewSuperStreamProducer(superStreamName, &stream.SuperStreamProducerOptions{
		RoutingStrategy: stream.NewHashRoutingMurmurStrategy(func(message message.StreamMessage) string {
			// here the code must fast and be safe
			// The evaluation is before send the message
			return message.GetApplicationProperties()["myKey"].(string)
		}),
		// Handle the confirmation of the message. It is mandatory for the super stream
		HandleSuperStreamConfirmation: func(partition string, confirmationStatus *stream.SuperStreamPublishConfirm) {
			for _, confirm := range confirmationStatus.ConfirmationStatus {
				if confirm.IsConfirmed() {
					fmt.Printf("Message with key: %s stored in partition %s\n", confirm.GetMessage().GetApplicationProperties()["myKey"], partition)
				} else {
					fmt.Printf("Message failed to be stored in partition %s\n", partition)
				}
			}
		},
		// HandlePartitionClose it not mandatory, but it is a good practice to handle it
		HandlePartitionClose: func(partition string, event stream.Event, context stream.PartitionContext) {
			// Here we deal with the partition close event
			// in case the connection is dropped due of network issues or metadata update
			// we can reconnect using context
			if strings.EqualFold(event.Reason, stream.SocketClosed) || strings.EqualFold(event.Reason, stream.MetaDataUpdate) {
				fmt.Printf("Partition %s closed unexpectedly.. Reconnecting..", partition)
				time.Sleep(3 * time.Second)
				err := context.ConnectPartition(partition)
				CheckErr(err)
			}
		},
		ClientProvidedName: "my-super-stream-producer",
	})
	CheckErr(err)

	// Publish messages
	for i := 0; i < 100; i++ {
		msg := amqp.NewMessage(make([]byte, 0))
		msg.ApplicationProperties = map[string]interface{}{"myKey": fmt.Sprintf("key_%d", i)}
		err = producer.Send(msg)
		CheckErr(err)
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press enter to exit")
	_, _ = reader.ReadString('\n')
	err = producer.Close()
	CheckErr(err)

}
