package test_helper

import (
	"bytes"
	"fmt"

	"github.com/onsi/gomega/types"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
)

type MessageDataMatcher struct {
	ExpectedData string
}

func HaveMatchingData(expected string) types.GomegaMatcher {
	return &MessageDataMatcher{
		ExpectedData: expected,
	}
}

func (matcher *MessageDataMatcher) Match(actual interface{}) (success bool, err error) {
	msg, ok := actual.(*amqp.Message)
	if !ok {
		return false, fmt.Errorf("HaveMatchingData matcher expects a *amqp.Message")
	}
	if msg == nil {
		return false, fmt.Errorf("HaveMatchingData matcher expects a non-nil *amqp.Message")
	}
	if msg.Data == nil {
		return false, fmt.Errorf("HaveMatchingData matcher expects a *amqp.Message with data")
	}
	var actualData []byte
	for _, data := range msg.Data {
		actualData = append(actualData, data...)
	}

	return bytes.Equal(actualData, []byte(matcher.ExpectedData)), nil
}

func (matcher *MessageDataMatcher) FailureMessage(actual interface{}) (message string) {
	msg := actual.(*amqp.Message)
	var actualData []byte
	for _, data := range msg.Data {
		actualData = append(actualData, data...)
	}
	return fmt.Sprintf("Expected\n\t%#v\nto have data matching\n\t%#v\nbut it was\n\t%#v", actual, matcher.ExpectedData, string(actualData))
}

func (matcher *MessageDataMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	msg := actual.(*amqp.Message)
	var actualData []byte
	for _, data := range msg.Data {
		actualData = append(actualData, data...)
	}
	return fmt.Sprintf("Expected\n\t%#v\nnot to have data matching\n\t%#v\nbut it was\n\t%#v", actual, matcher.ExpectedData, string(actualData))
}
