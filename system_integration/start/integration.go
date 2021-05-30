package main

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/ha"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"os"
	"strconv"
)

func checkErr(err error) {
	if err != nil {
		fmt.Printf("Test failed because of %s \n", err)
		os.Exit(1)
	}
}

func CreateArrayMessagesForTesting(numberOfMessages int) []*amqp.Message {
	var arr []*amqp.Message
	for z := 0; z < numberOfMessages; z++ {
		arr = append(arr, amqp.NewMessage([]byte("test_"+strconv.Itoa(z))))
	}
	return arr
}

func main() {
	// Set log level, not mandatory by default is INFO
	//s.SetLevelInfo(s.DEBUG)
	fmt.Println("Integration test")
	fmt.Println("Connecting to RabbitMQ streaming ...")

	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().
			SetHost("node0").
			SetUser("test").
			SetPassword("test"))
	checkErr(err)
	failOverProducer(env)
	//testCreateStreams(env)

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')

}

func failOverProducer(env *stream.Environment) {

	streamName := uuid.New().String()

	err := env.DeclareStream(streamName, nil)
	checkErr(err)

	rProducer := ha.New(env)

	err = rProducer.Start(streamName, "my_ha_producer")
	checkErr(err)

	data, err := env.StreamMetaData(streamName)
	checkErr(err)
	println(data)
	//err = system_integration.Operations{}.StopRabbitMQNode(data.Leader.Host)
	//checkErr(err)
	//time.Sleep(4 * time.Second)
	//err = system_integration.Operations{}.StartRabbitMQNode(data.Leader.Host)
	//checkErr(err)

	err = rProducer.Close()
	checkErr(err)

	err = env.DeleteStream(streamName)
	checkErr(err)

}

func testCreateStreams(env *stream.Environment) {
	fmt.Println("start execution create streams")
	var streams []string

	for i := 0; i < 50; i++ {
		streams = append(streams, uuid.New().String())
	}

	for _, s := range streams {
		err := env.DeclareStream(s, nil)
		checkErr(err)
	}
	var producers []*stream.Producer
	for _, s := range streams {
		producer1, err := env.NewProducer(s, nil)
		checkErr(err)
		producers = append(producers, producer1)
		_, err = producer1.BatchPublish(nil, CreateArrayMessagesForTesting(1000))
		checkErr(err)
	}

	if len(env.ClientCoordinator()) != 3 {
		checkErr(errors.New("ClientCoordinator fail"))
	}

	for _, s := range streams {
		if len(env.ProducerPerStream(s)) == 0 {
			checkErr(errors.New("ProducerPerStream fail"))
		}
	}

	for _, s := range streams {
		if len(env.ClientsPerStream(s)) == 0 {
			checkErr(errors.New("ClientsPerStream fail"))
		}
	}

	for _, s := range streams {
		err := env.DeleteStream(s)
		checkErr(err)
	}
	fmt.Println("end execution create streams")
}
