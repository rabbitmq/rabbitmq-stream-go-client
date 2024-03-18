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
	err = os.MkdirAll(getwd, os.ModePerm)
	if err != nil {
		fmt.Printf("can't create directory: %s", err)
	}

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
	rand.Seed(1)
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
	var uuidValue amqp.UUID // Value 00112233-4455-6677-8899-aabbccddeeff
	for i := byte(0); i < 16; i++ {
		uuidValue[i] = i<<4 | i
	}

	chineseStringTest :=
		"Alan Mathison Turing（1912 年 6 月 23 日 - 1954 年 6 月 7 日）是英国数学家、计算机科学家、逻辑学家、密码分析家、哲学家和理论生物学家。 [6] 图灵在理论计算机科学的发展中具有很大的影响力，用图灵机提供了算法和计算概念的形式化，可以被认为是通用计算机的模型。[7][8][9] 他被广泛认为是理论计算机科学和人工智能之父。 [10]"

	getwd, err := os.Getwd()
	if err != nil {
		return
	}
	getwd += "/generate" + "/" + "files"
	err = os.RemoveAll(getwd)
	err = os.Mkdir(getwd, 0755)

	msg := amqp.NewMessage([]byte(""))
	binary, err := msg.MarshalBinary()
	//msg.UnmarshalBinary()
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

	msg = amqp.NewMessage([]byte("this_is_a_amqp_message"))
	binary, err = msg.MarshalBinary()
	if err != nil {
		return
	}
	saveMessageToFile("message_body_this_is_a_amqp_message", binary)

	msg = amqp.NewMessage([]byte(chineseStringTest))
	binary, err = msg.MarshalBinary()
	if err != nil {
		return
	}
	saveMessageToFile("message_body_unicode_500_body", binary)

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
		generateString(7):   uuidValue,
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
		CorrelationID:      uuidValue,
		ContentType:        "json",
		ContentEncoding:    "myCoding",
		AbsoluteExpiryTime: time.Unix(1710440652, 0),
		CreationTime:       time.Unix(1710440652, 0),
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

	byteString := "Alan  Mathison Turing  ( 23 June 1912 – 7 June 1954 ) was an English  mathematician, computer scientist, logician, cryptanalyst,  philosopher, and theoretical biologist. Turing  was   highly  influential in the development of theoretical computer science."

	greekTest := "Ο Άλαν Μάθισον Τούρινγκ (23 Ιουνίου 1912 – 7 Ιουνίου 1954) ήταν Άγγλος μαθηματικός, επιστήμονας υπολογιστών, λογικός, κρυπαναλυτής, φιλόσοφος και θεωρητικός βιολόγος. Ο Τούρινγκ είχε μεγάλη επιρροή στην ανάπτυξη της θεωρητικής επιστήμης των υπολογιστών."
	msg = amqp.NewMessage([]byte(byteString))
	msg.ApplicationProperties = map[string]interface{}{
		"from_go":         "祝您有美好的一天，并享受客户",
		"from_go_ch_long": chineseStringTest,
		"from_go_greek":   greekTest,
		"from_go_byte":    byteString,
	}

	binary, err = msg.MarshalBinary()
	if err == nil {
		saveMessageToFile("message_unicode_message", binary)
	}

	msg = amqp.NewMessage([]byte(""))
	msg.Properties = &amqp.MessageProperties{
		MessageID:     uuidValue,
		CorrelationID: uuidValue,
	}
	binary, err = msg.MarshalBinary()
	if err == nil {
		saveMessageToFile("uuid_message", binary)
	}

	msg = amqp.NewMessage(nil)
	msg.Properties = &amqp.MessageProperties{
		MessageID:     nil,
		UserID:        nil,
		To:            "",
		Subject:       "",
		ReplyTo:       "",
		CorrelationID: nil,
	}
	msg.ApplicationProperties = map[string]interface{}{
		"empty":      "",
		"long_value": 91_000_001_001,
		"byte_value": byte(216),
		"bool_value": true,
		"int_value":  1,
		"float":      1.1,
		"double":     1.1,
		"uuid":       uuidValue,
		"null":       nil,
	}
	binary, err = msg.MarshalBinary()
	if err == nil {
		saveMessageToFile("nil_and_types", binary)
	}
}
