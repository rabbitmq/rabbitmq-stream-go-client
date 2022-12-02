package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/bombsimon/logrusr/v3"
	"github.com/go-logr/logr"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/common"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
	"github.com/sirupsen/logrus"
	"os"
	"time"
)

func main() {
	logrusLog := logrus.New()
	logrusLog.Level = logrus.DebugLevel
	log := logrusr.New(logrusLog).WithName("rabbitmq-stream")
	stream := "test-stream"
	config, err := raw.NewClientConfiguration("rabbitmq-stream://guest:guest@localhost:5552")
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	rabbitmqCtx := logr.NewContext(ctx, log)
	streamClient, err := raw.DialConfig(rabbitmqCtx, config)
	if err != nil {
		log.Error(err, "error in dial")
		panic(err)
	}
	log.Info("connection status", "open", streamClient.IsOpen())

	err = streamClient.DeclareStream(ctx, stream, map[string]string{"name": "test-stream"})
	if err != nil && err.Error() != "stream already exists" {
		log.Error(err, "error in declaring stream")
		panic(err)
	}

	err = streamClient.DeclarePublisher(ctx, 1, "test-publisher", stream)
	if err != nil {
		log.Error(err, "error in declaring publisher")
		panic(err)
	}

	fmt.Println("Start sending messages")
	var id uint64
	startTime := time.Now()
	for j := 0; j < 10_000; j++ {
		var fakeMessages []common.PublishingMessager
		for i := 0; i < 100; i++ {
			fakeMessages = append(fakeMessages,
				raw.NewPublishingMessage(id, NewFakeMessage([]byte(fmt.Sprintf("message %d", i)))))

			id++ // increment the id
		}

		err = streamClient.Send(ctx, 1, fakeMessages)
		if err != nil {
			log.Error(err, "error in sending messages")
			panic(err)
		}
	}
	fmt.Println("End sending messages")
	fmt.Printf("Sent %d  in : %s \n", id, time.Since(startTime))

	fmt.Println("Press any key to stop ")
	reader := bufio.NewReader(os.Stdin)
	_, _ = reader.ReadString('\n')
	err = streamClient.DeletePublisher(ctx, 1)
	if err != nil {
		log.Error(err, "error in deleting publisher")
		panic(err)
	}

	err = streamClient.DeleteStream(ctx, stream)
	if err != nil {
		return
	}

	log.Info("closing connection")
	err = streamClient.Close(ctx)
	if err != nil {
		log.Error(err, "error closing connection")
		panic(err)
	}
	log.Info("connection status", "open", streamClient.IsOpen())
}

type FakeMessage struct {
	body []byte
}

func (f *FakeMessage) Write(writer *bufio.Writer) (int, error) {
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
	return written, nil
}

func (f *FakeMessage) SetBody(body []byte) {
	f.body = body
}

func (f *FakeMessage) GetBody() []byte {
	return f.body
}

func NewFakeMessage(body []byte) *FakeMessage {
	return &FakeMessage{body: body}
}
