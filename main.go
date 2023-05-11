package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/common"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	"golang.org/x/exp/slog"
	"io"
	"os"
	"time"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout))

	stream := "test-stream"
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

	err = streamClient.DeclareStream(ctx, stream, map[string]string{"name": "test-stream"})
	if err != nil && err.Error() != "stream already exists" {
		log.Error("error in declaring stream", "error", err)
		panic(err)
	}

	log.Info("exchanging command versions with server")
	err = streamClient.ExchangeCommandVersions(ctx)
	if err != nil {
		log.Error("error in exchange command versions", "error", err)
		panic(err)
	}

	const batchSize = 100
	const iterations = 2
	const totalMessages = iterations * batchSize
	// PublishingId 555 --> [] messages 551, 552, 553, 554, 555
	// ONLY 555
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

	err = streamClient.DeclarePublisher(ctx, 1, "test-publisher", stream)
	if err != nil {
		log.Error("error in declaring publisher", "error", err)
		panic(err)
	}

	fmt.Println("Start sending messages")
	var id uint64
	startTime := time.Now()
	for j := 0; j < iterations; j++ {
		var fakeMessages []common.PublishingMessager
		for i := 0; i < batchSize; i++ {
			fakeMessages = append(fakeMessages,
				raw.NewPublishingMessage(id, NewFakeMessage([]byte(fmt.Sprintf("message %d", i)))))
			id++ // increment the id
		}

		err = streamClient.Send(ctx, 1, fakeMessages)
		if err != nil {
			log.Error("error in sending messages", "error", err)
			panic(err)
		}
	}
	fmt.Println("End sending messages")
	fmt.Printf("Sent %d  in : %s \n", id, time.Since(startTime))

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

	err = streamClient.Subscribe(ctx, stream, constants.OffsetTypeFirst, 1, 10, map[string]string{"name": "my_consumer"}, 10)
	if err != nil {
		panic(err)
	}

	offset, err := streamClient.QueryOffset(ctx, "my_consumer", stream)
	if err != nil {
		log.Error("error querying offset", "error", err)
	} else {
		log.Info("offset", "offset", offset)
	}

	// sending sub-entry batch to the server
	var subEntryFakeMessages []common.PublishingMessager
	for i := 0; i < batchSize; i++ {
		subEntryFakeMessages = append(subEntryFakeMessages,
			raw.NewPublishingMessage(id, NewFakeMessage([]byte(fmt.Sprintf("message %d", i)))))
		id++ // increment the id
	}

	err = streamClient.DeclarePublisher(ctx, 2, "test-publisher-subEntry", stream)
	if err != nil {
		log.Error("error in declaring publisher", "error", err)
		panic(err)
	}
	var idSub uint64 = 0

	err = streamClient.SendSubEntryBatch(ctx,
		2,
		idSub,
		&common.CompressNONE{},
		subEntryFakeMessages)

	if err != nil {
		log.Error("error in SendSubEntryBatch", "error", err)
		panic(err)
	}

	idSub = idSub + 1
	err = streamClient.SendSubEntryBatch(ctx,
		2,
		idSub,
		&common.CompressGZIP{},
		subEntryFakeMessages)

	if err != nil {
		log.Error("error in SendSubEntryBatch", "error", err)
		panic(err)
	}

	fmt.Println("Press any key to stop ")
	reader := bufio.NewReader(os.Stdin)
	_, _ = reader.ReadString('\n')
	err = streamClient.DeletePublisher(ctx, 1)
	if err != nil {
		log.Error("error in deleting publisher", "error", err)
		panic(err)
	}

	err = streamClient.DeleteStream(ctx, stream)
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

type FakeMessage struct {
	body []byte
}

func (f *FakeMessage) WriteTo(writer io.Writer) (int64, error) {
	written := 0
	err := binary.Write(writer, binary.BigEndian, uint32(len(f.body)))
	if err != nil {
		panic(err)
	}
	written += binary.Size(uint32(len(f.body)))

	err = binary.Write(writer, binary.BigEndian, f.body)
	if err != nil {
		panic(err)
	}
	written += binary.Size(f.body)
	return int64(written), nil
}

func (f *FakeMessage) SetBody(body []byte) {
	f.body = body
}

func (f *FakeMessage) Body() []byte {
	return f.body
}

func NewFakeMessage(body []byte) *FakeMessage {
	return &FakeMessage{body: body}
}
