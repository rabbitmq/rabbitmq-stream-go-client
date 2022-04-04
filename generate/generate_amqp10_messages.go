package main

import (
	"fmt"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"math/rand"
	"os"
	"time"
)

func saveMessageToFile(filename string, data []byte) {
	getwd, err := os.Getwd()
	if err != nil {
		return
	}
	getwd += "/generate" + "/" + "files"

	outf, err := os.Create(getwd + "/" + filename)
	if err != nil {
		fmt.Printf("can't create file: %s", err)
	}
	_, err = outf.Write(data)
	if err != nil {
		fmt.Printf("can't write file: %s", err)
	}
	err = outf.Close()
	if err != nil {
		fmt.Printf("can't close file: %s", err)
	}

}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func generateString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func main() {

	getwd, err := os.Getwd()
	if err != nil {
		return
	}
	getwd += "/generate" + "/" + "files"
	err = os.RemoveAll(getwd)
	err = os.Mkdir(getwd, 0755)

	msg := amqp.NewMessage([]byte(""))
	binary, err := msg.MarshalBinary()
	if err != nil {
		return
	}
	saveMessageToFile("empty_message", binary)

	msg = amqp.NewMessage([]byte(generateString(250)))
	binary, err = msg.MarshalBinary()
	if err != nil {
		return
	}
	saveMessageToFile("message_body_250", binary)

	msg = amqp.NewMessage([]byte(generateString(700)))
	binary, err = msg.MarshalBinary()
	if err != nil {
		return
	}
	saveMessageToFile("message_body_700", binary)

	msg = amqp.NewMessage([]byte(generateString(300)))
	msg.ApplicationProperties = map[string]interface{}{
		generateString(30):  generateString(250),
		generateString(40):  generateString(50),
		generateString(200): generateString(250),
		generateString(100): 2,
		generateString(220): 2_000_000_000_000,
		generateString(3):   byte(1),
		generateString(4):   int16(12),
		generateString(5):   int32(6000),
		generateString(6):   int64(6_000_000),
	}
	binary, err = msg.MarshalBinary()
	if err != nil {
		return
	}
	saveMessageToFile("message_random_application_properties_300", binary)

	msg = amqp.NewMessage([]byte(generateString(800)))
	msg.ApplicationProperties = map[string]interface{}{
		generateString(530): generateString(500),
		generateString(40):  generateString(700),
		generateString(200): generateString(499),
	}
	binary, err = msg.MarshalBinary()
	if err != nil {
		return
	}
	saveMessageToFile("message_random_application_properties_500", binary)

	msg = amqp.NewMessage([]byte(generateString(900)))
	msg.Properties = &amqp.MessageProperties{
		MessageID:          uint64(33333333),
		UserID:             nil,
		To:                 generateString(50),
		Subject:            generateString(30),
		ReplyTo:            generateString(30),
		CorrelationID:      nil,
		ContentType:        "json",
		ContentEncoding:    "myCoding",
		AbsoluteExpiryTime: time.Now(),
		CreationTime:       time.Now(),
		GroupID:            generateString(50),
		GroupSequence:      10,
		ReplyToGroupID:     generateString(50),
	}
	msg.ApplicationProperties = map[string]interface{}{
		generateString(900): generateString(900),
	}
	binary, err = msg.MarshalBinary()
	if err != nil {
		return
	}
	saveMessageToFile("message_random_application_properties_properties_900", binary)

	msg = amqp.NewMessage([]byte("test"))
	msg.Properties = &amqp.MessageProperties{
		MessageID:          "test",
		UserID:             []byte("test"),
		To:                 "test",
		Subject:            "test",
		ReplyTo:            "test",
		CorrelationID:      1,
		ContentType:        "test",
		ContentEncoding:    "test",
		AbsoluteExpiryTime: time.Time{},
		CreationTime:       time.Time{},
		GroupID:            "test",
		GroupSequence:      1,
		ReplyToGroupID:     "test",
	}
	msg.ApplicationProperties = map[string]interface{}{
		"test": "test",
	}
	msg.Annotations = map[interface{}]interface{}{
		"test":  "test",
		1:       1,
		100_000: 100_000,
	}
	binary, err = msg.MarshalBinary()
	if err != nil {
		return
	}
	saveMessageToFile("static_test_message_compare", binary)

}
