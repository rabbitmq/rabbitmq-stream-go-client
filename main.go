package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/bombsimon/logrusr/v3"
	"github.com/go-logr/logr"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/stream"
	"github.com/sirupsen/logrus"
	"os"
)

func main() {
	logrusLog := logrus.New()
	logrusLog.Level = logrus.DebugLevel
	log := logrusr.New(logrusLog).WithName("rabbitmq-stream")

	config, err := stream.NewRawClientConfiguration("rabbitmq-stream://guest:guest@localhost:5552")
	if err != nil {
		panic(err)
	}

	var streamClient stream.Clienter
	ctx := context.Background()
	rabbitmqCtx := logr.NewContext(ctx, log)
	streamClient, err = stream.DialConfig(rabbitmqCtx, config)
	if err != nil {
		log.Error(err, "error in dial")
		panic(err)
	}

	log.Info("connection status", "open", streamClient.IsOpen())

	fmt.Println("Press any key to stop ")
	reader := bufio.NewReader(os.Stdin)
	_, _ = reader.ReadString('\n')

	log.Info("closing connection")
	err = streamClient.Close(ctx)
	if err != nil {
		log.Error(err, "error closing connection")
		panic(err)
	}
	log.Info("connection status", "open", streamClient.IsOpen())
}
