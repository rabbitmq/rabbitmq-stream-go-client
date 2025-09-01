package stream

import (
	"fmt"
	"strings"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/message"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
)

type responseError struct {
	Err       error
	isTimeout bool
}

func newResponseError(err error, timeout bool) responseError {
	return responseError{
		Err:       err,
		isTimeout: timeout,
	}
}

func uShortExtractResponseCode(code uint16) uint16 {
	return code & 0b0111_1111_1111_1111
}

// func UIntExtractResponseCode(code int32) int32 {
//	return code & 0b0111_1111_1111_1111
//}

func uShortEncodeResponseCode(code uint16) uint16 {
	return code | 0b1000_0000_0000_0000
}

func waitCodeWithDefaultTimeOut(response *Response) responseError {
	return waitCodeWithTimeOut(response, defaultSocketCallTimeout)
}
func waitCodeWithTimeOut(response *Response, timeout time.Duration) responseError {
	select {
	case code := <-response.code:
		if code.id != responseCodeOk {
			return newResponseError(lookErrorCode(code.id), false)
		}
		return newResponseError(nil, false)
	case <-time.After(timeout):
		logs.LogError("timeout %d ns - waiting Code, operation: %s", timeout.Milliseconds(), response.commandDescription)

		return newResponseError(
			fmt.Errorf("timeout %d ms - waiting Code, operation: %s ",
				timeout.Milliseconds(), response.commandDescription), true)
	}
}

func SetLevelInfo(value int8) {
	logs.LogLevel = value
}

func containsOnlySpaces(input string) bool {
	return len(input) > 0 && len(strings.TrimSpace(input)) == 0
}

/// utils // for testing purposes

func sendToSuperStream(env *Environment, superStream string) error {
	signal := make(chan struct{})
	superProducer, err := env.NewSuperStreamProducer(superStream,
		&SuperStreamProducerOptions{
			RoutingStrategy: NewHashRoutingStrategy(func(message message.StreamMessage) string {
				return message.GetApplicationProperties()["routingKey"].(string)
			})})
	if err != nil {
		return fmt.Errorf("error creating super stream producer: %w", err)
	}

	go func(ch <-chan PartitionPublishConfirm) {
		recv := 0
		for confirm := range ch {
			recv += len(confirm.ConfirmationStatus)
			if recv == 20 {
				signal <- struct{}{}
			}
		}
	}(superProducer.NotifyPublishConfirmation(1))

	for i := range 20 {
		msg := amqp.NewMessage(make([]byte, 0))
		msg.ApplicationProperties = map[string]any{"routingKey": fmt.Sprintf("hello%d", i)}
		err = superProducer.Send(msg)
		if err != nil {
			return fmt.Errorf("error sending message to super stream: %w", err)
		}
	}
	<-signal
	close(signal)
	err = superProducer.Close()
	if err != nil {
		return err
	}
	return nil
}
