package ha

import (
	"errors"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
	"math/rand"
	"time"
)

const (
	StatusOpen               = 1
	StatusClosed             = 2
	StatusStreamDoesNotExist = 3
	StatusReconnecting       = 4
)

type newEntityInstance func() error

type Reliabler interface {
	setStatus(value int)
	getInfo() string
	getEnv() *stream.Environment
	getNewInstance() newEntityInstance
	getTimeOut() time.Duration
	getStreamName() string
}

func retry(backoff int, reliabler Reliabler) (error, bool) {
	reliabler.setStatus(StatusReconnecting)
	sleepValue := rand.Intn(int((reliabler.getTimeOut().Seconds()-2+1)+2)*1000) + backoff*1000
	logs.LogInfo("[Reliable] - The %s for the stream %s is in reconnection in %d milliseconds", reliabler.getInfo(), reliabler.getStreamName(), sleepValue)
	time.Sleep(time.Duration(sleepValue) * time.Millisecond)
	streamMetaData, errS := reliabler.getEnv().StreamMetaData(reliabler.getStreamName())
	if errors.Is(errS, stream.StreamDoesNotExist) {
		return errS, true
	}
	if errors.Is(errS, stream.StreamNotAvailable) {
		logs.LogInfo("[Reliable] - The stream %s is not available for %s. Trying to reconnect", reliabler.getStreamName(), reliabler.getInfo())
		return retry(backoff+1, reliabler)
	}
	if streamMetaData.Leader == nil {
		logs.LogInfo("[Reliable] - The leader for the stream %s is not ready for %s. Trying to reconnect", reliabler.getStreamName(), reliabler.getInfo())
		return retry(backoff+1, reliabler)
	}

	var result error
	if streamMetaData != nil {
		logs.LogInfo("[Reliable] - The stream %s exists. Reconnecting the producer %s.", reliabler.getStreamName(), reliabler.getInfo())
		result = reliabler.getNewInstance()()
		if result == nil {
			logs.LogInfo("[Reliable] - The stream %s exists. Producer %s reconnected.", reliabler.getInfo(), reliabler.getStreamName())
		} else {
			logs.LogInfo("[Reliable] - error %s creating producer %s for the stream %s. Trying to reconnect", result, reliabler.getInfo(), reliabler.getStreamName())
			return retry(backoff+1, reliabler)
		}
	} else {
		logs.LogError("[Reliable] - The stream %s does not exist for %s. Closing..", reliabler.getStreamName(), reliabler.getInfo())
		result = stream.StreamDoesNotExist
	}

	return result, true

}
