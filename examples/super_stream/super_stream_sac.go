package main

import (
	"bufio"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"math/rand"
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

	// *** RUN the producer first *****
	// Example for super stream with single active consumer
	// with EnableSingleActiveConsumer is possible to enable/disable the single active consumer feature
	const EnableSingleActiveConsumer = true

	//stream.SetLevelInfo(logs.DEBUG)
	appName := "MyApplication"

	fmt.Printf("Super stream consumer example - Single Active Consumer active: %t\n", EnableSingleActiveConsumer)
	fmt.Println("Connecting to RabbitMQ streaming ...")

	// Connect to the broker ( or brokers )
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions())
	CheckErr(err)
	superStreamName := "MySuperStream"

	// Create a super stream
	err = env.DeclareSuperStream(superStreamName,
		// the partitions strategy is mandatory
		// can be partition or by key ( see BindingsOptions)
		// In this case we create a super stream with 3 partitions
		stream.NewPartitionsOptions(3).
			SetMaxLengthBytes(stream.ByteCapacity{}.GB(3)))
	CheckErr(err)

	handleMessages := func(consumerContext stream.ConsumerContext, message *amqp.Message) {
		fmt.Printf("[%s] - [ partition: %s] consumer name: %s, data: %s, message offset %d, \n ",
			time.Now().Format(time.TimeOnly),
			consumerContext.Consumer.GetStreamName(),
			consumerContext.Consumer.GetName(), message.Data, consumerContext.Consumer.GetOffset())
		// This is only for the example, in a real application you should not store the offset
		// for each message, it is better to store the offset for a batch of messages
		err := consumerContext.Consumer.StoreOffset()
		CheckErr(err)
	}

	// Create a single active consumer struct
	// In this example, we set the offset to the last one stored
	sac := stream.NewSingleActiveConsumer(
		func(partition string, isActive bool) stream.OffsetSpecification {
			// This function is called when the consumer is promoted to active
			// or not active anymore
			restart := stream.OffsetSpecification{}.First()
			offset, err := env.QueryOffset(appName, partition)
			if err == nil {
				restart = stream.OffsetSpecification{}.Offset(offset + 1)
			}

			addInfo := fmt.Sprintf("The cosumer is now active ....Restarting from offset: %s", restart)
			if !isActive {
				addInfo = "The consumer is not active anymore for this partition."
			}

			fmt.Printf("[%s] - Consumer update for: %s. %s\n", time.Now().Format(time.TimeOnly),
				partition, addInfo)

			return restart
		},
	)

	// Create a superStreamConsumer
	superStreamConsumer, err := env.NewSuperStreamConsumer(superStreamName, handleMessages,
		stream.NewSuperStreamConsumerOptions().
			SetSingleActiveConsumer(sac.
				// by default the single active consumer is enabled
				// This flag is used to enable/disable the single active consumer feature
				// In normal use cases, it is not needed to disable it
				SetEnabled(EnableSingleActiveConsumer)).

			// The consumer name is mandatory to enable the single active consumer
			//  and _must_ be the same for all the consumers in the same group
			SetConsumerName(appName).
			SetOffset(stream.OffsetSpecification{}.First()))

	CheckErr(err)

	// HandlePartitionClose it not mandatory, but it is a good practice to handle it
	go func(ch <-chan stream.CPartitionClose) {
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

				restart := stream.OffsetSpecification{}.First()
				offset, err := env.QueryOffset(appName, partitionCloseEvent.Partition)
				if err == nil {
					restart = stream.OffsetSpecification{}.Offset(offset + 1)
				}

				err = partitionCloseEvent.Context.ConnectPartition(partitionCloseEvent.Partition, restart)
				// tries only one time. Good for testing not enough for real use case
				CheckErr(err)
				fmt.Printf("Partition %s reconnected.\n", partitionCloseEvent.Partition)
			}
		}
	}(superStreamConsumer.NotifyPartitionClose(1))

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press enter to close the consumer")
	_, _ = reader.ReadString('\n')
	err = superStreamConsumer.Close()
	CheckErr(err)
}
