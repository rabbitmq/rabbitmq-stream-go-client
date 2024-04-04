package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func CheckErr(err error) {
	if err != nil {
		fmt.Printf("%s ", err)
		os.Exit(1)
	}
}
func main() {
	// Example for super stream with partitions
	fmt.Println("Super stream example - partitions")
	fmt.Println("Connecting to RabbitMQ streaming ...")
	stream.SetLevelInfo(logs.DEBUG)

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
	err = env.DeclareSuperStream(superStreamName,
		// the partitions strategy is mandatory
		// can be partition or by key ( see BindingsOptions)
		// In this case we create a super stream with 3 partitions
		stream.NewPartitionsOptions(3).
			SetMaxLengthBytes(stream.ByteCapacity{}.GB(3)))
	CheckErr(err)

	// Create a superStreamProducer
	superStreamProducer, err := env.NewSuperStreamProducer(superStreamName,
		stream.NewSuperStreamProducerOptions(
			stream.NewHashRoutingStrategy(func(message message.StreamMessage) string {
				// here the code must fast and be safe
				// The code evaluation is before sending the message
				return message.GetApplicationProperties()["myKey"].(string)
			})).SetClientProvidedName("my-superStreamProducer"))
	CheckErr(err)

	// HandlePartitionClose it not mandatory, but it is a good practice to handle it
	go func(ch <-chan stream.PartitionClose) {
		// Here we deal with the partition close event
		// in case the connection is dropped due of network issues or metadata update
		// we can reconnect using context
		for partitionCloseEvent := range ch {
			// important to check the event Reason. SocketClosed and MetaDataUpdate
			// are usually unexpected reasons
			if strings.EqualFold(partitionCloseEvent.Event.Reason, stream.SocketClosed) || strings.EqualFold(partitionCloseEvent.Event.Reason, stream.MetaDataUpdate) {
				// A random sleep is recommended to avoid to try too often.
				// avoid to reconnect in the same time in case there are multiple clients
				sleepValue := rand.Intn(5) + 2
				fmt.Printf("Partition %s closed unexpectedly! Reconnecting in %v seconds..\n", partitionCloseEvent.Partition, sleepValue)
				time.Sleep(time.Duration(sleepValue) * time.Second)
				err := partitionCloseEvent.Context.ConnectPartition(partitionCloseEvent.Partition)
				// tries only one time. Good for testing not enough for real use case
				CheckErr(err)
				fmt.Printf("Partition %s reconnected.\n", partitionCloseEvent.Partition)
			}
		}
	}(superStreamProducer.NotifyPartitionClose())

	var confirmed int32
	go func(ch <-chan stream.PartitionPublishConfirm) {
		for superStreamPublishConfirm := range ch {
			for _, confirm := range superStreamPublishConfirm.ConfirmationStatus {
				if confirm.IsConfirmed() {
					fmt.Printf("Message with key: %s stored in partition %s, total: %d\n",
						confirm.GetMessage().GetApplicationProperties()["myKey"],
						superStreamPublishConfirm.Partition,
						atomic.AddInt32(&confirmed, 1))
				} else {
					// here you should store the message in another list and try again
					// like unConfirmed.append(msg...) messages ...
					// In this example we won't handle it to leave it simple
					// the messages can't be stored for different reasons ( see the ConfirmationStatus for more details)
					fmt.Printf("Message failed to be stored in partition %s\n", superStreamPublishConfirm.Partition)
				}
			}
		}
	}(superStreamProducer.NotifyPublishConfirmation())

	// Publish messages
	for i := 0; i < 500; i++ {
		msg := amqp.NewMessage(make([]byte, 0))
		msg.ApplicationProperties = map[string]interface{}{"myKey": fmt.Sprintf("key_%d", i)}
		err = superStreamProducer.Send(msg)
		if errors.Is(err, stream.ErrProducerNotFound) {
			// that's can be a temp situation.
			// maybe the producer is in reconnection due of unexpected disconnection
			// it is up to the user to decide what to do.
			// In this can we can ignore the log and continue to send messages
			logs.LogError("can't send the message ... the producer was not found")
			// here you should store the message in another list and try again
			// like unConfirmed.append(msg...) messages ...
			// In this example we won't handle it to leave it simple
			// like the superStreamPublishConfirm event for  messages
		} else {
			CheckErr(err)
		}

		time.Sleep(200 * time.Millisecond)
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press enter to close the producer")
	_, _ = reader.ReadString('\n')
	err = superStreamProducer.Close()
	CheckErr(err)
	fmt.Println("Press enter to exit")
	_, _ = reader.ReadString('\n')

}
