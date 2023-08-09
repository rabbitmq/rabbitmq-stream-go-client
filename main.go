package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/codecs/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/common"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/constants"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/raw"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/stream"
	"log/slog"
	"os"
	"time"
)

func main() {
	runSmartClient()
	runRawClient()
}

func runSmartClient() {
	slogOpts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, slogOpts))

	ctx := raw.NewContextWithLogger(context.Background(), *log)

	c := stream.NewEnvironmentConfiguration(
		stream.WithLazyInitialization(false),
		stream.WithUri("rabbitmq-stream://localhost:5552"),
	)

	env, err := stream.NewEnvironment(ctx, c)
	if err != nil {
		panic(err)
	}
	//defer env.Close()

	err = env.CreateStream(ctx, "my-stream", stream.CreateStreamOptions{})
	if err != nil {
		panic(err)
	}

	sc := bufio.NewScanner(os.Stdin)
	fmt.Print("Close the connection and press enter")
	sc.Scan()

	err = env.DeleteStream(ctx, "my-stream")
	if err != nil {
		panic(err)
	}

	err = env.CreateStream(ctx, "other-stream", stream.CreateStreamOptions{})
	if err != nil {
		panic(err)
	}

	fmt.Print("Life good! Press enter to exit")
	sc.Scan()
	err = env.DeleteStream(ctx, "other-stream")
	if err != nil {
		panic(err)
	}
}

func runRawClient() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	streamName := "test-streamName"
	config, err := raw.NewClientConfiguration("rabbitmq-stream://guest:guest@localhost:5552")
	if err != nil {
		panic(err)
	}

	config.SetConnectionName("test-connection")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rabbitmqCtx := raw.NewContextWithLogger(ctx, *log)
	streamClient, err := raw.DialConfig(rabbitmqCtx, config)
	if err != nil {
		log.Error("error in dial", "error", err)
		panic(err)
	}
	log.Info("connection status", "open", streamClient.IsOpen())

	var closeChan = streamClient.NotifyConnectionClosed()
	go func() {
		for c := range closeChan {
			log.Info("connection closed", "reason", c, "isOpen", streamClient.IsOpen())
		}
	}()

	err = streamClient.DeclareStream(ctx, streamName, map[string]string{"name": "test-streamName"})
	if err != nil && err.Error() != "streamName already exists" {
		log.Error("error in declaring streamName", "error", err)
		panic(err)
	}

	log.Info("exchanging command versions with server")
	err = streamClient.ExchangeCommandVersions(ctx)
	if err != nil {
		log.Error("error in exchange command versions", "error", err)
		panic(err)
	}

	const batchSize = 100
	const iterations = 1000
	const totalMessages = iterations * batchSize
	publishChan := streamClient.NotifyPublish(make(chan *raw.PublishConfirm, 100))
	go func() {
		var confirmed int
		for c := range publishChan {
			switch c.PublisherID() {
			case 1:
				confirmed += len(c.PublishingIds())
				if (confirmed % totalMessages) == 0 {
					log.Info("Confirmed", "messages", confirmed)
				}
			case 2:
				confirmed += len(c.PublishingIds())
				log.Info("Sub Entry Send Confirmed", "messages", confirmed)
			}

		}
	}()

	err = streamClient.DeclarePublisher(ctx, 1, "test-publisher", streamName)
	if err != nil {
		log.Error("error in declaring publisher", "error", err)
		panic(err)
	}

	fmt.Println("Start sending messages")
	var id uint64
	startTime := time.Now()
	for j := 0; j < iterations; j++ {
		var messages []common.PublishingMessager
		for i := 0; i < batchSize; i++ {
			msg := amqp.NewAMQP10Message([]byte(fmt.Sprintf("msg %d", i)))

			messages = append(messages,
				raw.NewPublishingMessage(id, msg))
			id++ // increment the id
		}

		err = streamClient.Send(ctx, 1, messages)
		if err != nil {
			log.Error("error in sending messages", "error", err)
			panic(err)
		}
	}
	fmt.Println("End sending messages")
	fmt.Printf("Sent %d  in : %s \n", id, time.Since(startTime))

	/// BATCH SEND
	fmt.Println("Start sending BATCH messages")

	err = streamClient.DeclarePublisher(ctx, 2, "test-publisher-subEntry", streamName)
	if err != nil {
		log.Error("error in declaring publisher", "error", err)
		panic(err)
	}
	var idSub uint64 = 0

	startTime = time.Now()
	// sending sub-entry batch to the server
	var messages []common.Message
	for i := 0; i < batchSize; i++ {
		messages = append(messages,
			amqp.NewAMQP10Message([]byte(fmt.Sprintf("message %d", i))))
	}

	err = streamClient.SendSubEntryBatch(ctx, 2,
		idSub,
		&common.CompressNONE{},
		messages)

	if err != nil {
		log.Error("error in SendSubEntryBatch", "error", err)
		panic(err)
	}

	idSub = idSub + 1
	err = streamClient.SendSubEntryBatch(ctx, 2,
		idSub, &common.CompressGZIP{}, messages)

	if err != nil {
		log.Error("error in SendSubEntryBatch", "error", err)
		panic(err)
	}

	fmt.Printf("Sent %d  in : %s \n", batchSize*2, time.Since(startTime))

	/// END BATCH SEND

	var received int
	chunkChan := streamClient.NotifyChunk(make(chan *raw.Chunk, 100))
	go func() {
		for c := range chunkChan {
			received += int(c.NumEntries)
			err := streamClient.Credit(ctx, 1, 1)
			if err != nil {
				log.Error("error sending credits", "error", err)
			}
			if (received % totalMessages) == 0 {
				log.Info("Received", "messages ", received)
			}
		}
	}()

	err = streamClient.Subscribe(ctx, streamName, constants.OffsetTypeFirst, 1, 10, map[string]string{"name": "my_consumer"}, 10)
	if err != nil {
		panic(err)
	}

	offset, err := streamClient.QueryOffset(ctx, "my_consumer", streamName)
	if err != nil {
		log.Error("error querying offset", "error", err)
	} else {
		log.Info("offset", "offset", offset)
	}

	fmt.Println("Press any key to stop ")
	reader := bufio.NewReader(os.Stdin)
	_, _ = reader.ReadString('\n')
	err = streamClient.DeletePublisher(ctx, 1)
	if err != nil {
		log.Error("error in deleting publisher", "error", err)
		panic(err)
	}

	err = streamClient.DeleteStream(ctx, streamName)
	if err != nil {
		return
	}

	log.Info("closing connection")
	err = streamClient.Close(ctx)
	if err != nil {
		log.Error("error closing connection", "error", err)
		panic(err)
	}
	log.Info("connection status", "open", streamClient.IsOpen())
}
