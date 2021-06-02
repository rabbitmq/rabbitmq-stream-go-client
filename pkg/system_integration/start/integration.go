package main

import (
	"bufio"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	system_integration2 "github.com/rabbitmq/rabbitmq-stream-go-client/pkg/system_integration"
	"log"
	"os"
	"strconv"
	"time"
)

func LogInfo(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[info] - %s", message), v...)
}

func LogError(message string, v ...interface{}) {
	log.Printf(fmt.Sprintf("[error] - %s", message), v...)
}

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
	stream.SetLevelInfo(logs.DEBUG)
	env, err := stream.NewEnvironment(
		stream.NewEnvironmentOptions().SetUris([]string{
			"rabbitmq-stream://test:test@node0:5552/%2f",
			"rabbitmq-stream://test:test@node1:5552/%2f",
			"rabbitmq-stream://test:test@node2:5552/%2f"}).
			SetMaxProducersPerClient(3))
	checkErr(err)
	deleteStream(env)
	failOverCluster(env)
	nodes := []string{"node1", "node0", "node1"}
	for _, node := range nodes {
		LogInfo("**************************************************************************")
		LogInfo("fail Over Cluster with node stopped: %s ", node)
		err = system_integration2.Operations{}.StopRabbitMQNode(node)
		checkErr(err)
		failOverCluster(env)
		err = system_integration2.Operations{}.StartRabbitMQNode(node)
		checkErr(err)

	}

	//testCreateStreams(env)

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Press any key to stop ")
	_, _ = reader.ReadString('\n')

}

func deleteStream(env *stream.Environment) {
	LogInfo("**************************************************************************")
	LogInfo("delete Stream")
	streamName := uuid.New().String()

	err := env.DeclareStream(streamName, nil)
	checkErr(err)

	rProducer := stream.NewHAProducer(env)
	err = rProducer.NewProducer(streamName, "producer-ha-test")
	checkErr(err)

	if !rProducer.IsOpen() {
		checkErr(errors.New("Producer is closed"))
	}
	err = env.DeleteStream(streamName)
	checkErr(err)

	if rProducer.IsOpen() {
		checkErr(errors.New("Producer is open"))
	}

	err = rProducer.Close()
	checkErr(err)

}

func failOverCluster(env *stream.Environment) {
	for i := 0; i < 2; i++ {

		LogInfo("**************************************************************************")
		LogInfo("fail Over Cluster: %d ", i)
		streamName := uuid.New().String()

		err := env.DeclareStream(streamName, nil)
		checkErr(err)

		rProducer := stream.NewHAProducer(env)
		err = rProducer.NewProducer(streamName, "producer-ha-test")
		checkErr(err)

		if !rProducer.IsOpen() {
			checkErr(errors.New("Producer is closed"))
		}

		data, err := env.StreamMetaData(streamName)
		checkErr(err)

		LogInfo("stopping node %s", data.Leader.Host)
		err = system_integration2.Operations{}.StopRabbitMQNode(data.Leader.Host)
		checkErr(err)
		if !rProducer.IsOpen() {
			checkErr(errors.New("Producer is closed"))
		}
		time.Sleep(1 * time.Second)
		LogInfo("starting node %s", data.Leader.Host)
		err = system_integration2.Operations{}.StartRabbitMQNode(data.Leader.Host)
		checkErr(err)

		err = rProducer.Close()
		checkErr(err)

		err = env.DeleteStream(streamName)
		checkErr(err)

		if rProducer.IsOpen() {
			checkErr(errors.New("Producer is open"))
		}
	}

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
