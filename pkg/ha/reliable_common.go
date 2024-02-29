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

type IReliable interface {
	setStatus(value int)
	GetStatus() int
	getInfo() string
	getEnv() *stream.Environment
	getNewInstance() newEntityInstance
	getTimeOut() time.Duration
	getStreamName() string
}

func retry(backoff int, reliable IReliable) (error, bool) {
	reliable.setStatus(StatusReconnecting)
	sleepValue := rand.Intn(int((reliable.getTimeOut().Seconds()-2+1)+2)*1000) + backoff*1000
	logs.LogInfo("[Reliable] - The %s for the stream %s is in reconnection in %d milliseconds", reliable.getInfo(), reliable.getStreamName(), sleepValue)
	time.Sleep(time.Duration(sleepValue) * time.Millisecond)
	streamMetaData, errS := reliable.getEnv().StreamMetaData(reliable.getStreamName())
	if errors.Is(errS, stream.StreamDoesNotExist) {
		return errS, true
	}
	if errors.Is(errS, stream.StreamNotAvailable) {
		logs.LogInfo("[Reliable] - The stream %s is not available for %s. Trying to reconnect", reliable.getStreamName(), reliable.getInfo())
		return retry(backoff+1, reliable)
	}
	if streamMetaData.Leader == nil {
		logs.LogInfo("[Reliable] - The leader for the stream %s is not ready for %s. Trying to reconnect", reliable.getStreamName(), reliable.getInfo())
		return retry(backoff+1, reliable)
	}

	var result error
	if streamMetaData != nil {
		logs.LogInfo("[Reliable] - The stream %s exists. Reconnecting the consumer %s.", reliable.getStreamName(), reliable.getInfo())
		result = reliable.getNewInstance()()
		if result == nil {
			logs.LogInfo("[Reliable] - The stream %s exists. Producer %s reconnected.", reliable.getInfo(), reliable.getStreamName())
		} else {
			logs.LogInfo("[Reliable] - error %s creating consumer %s for the stream %s. Trying to reconnect", result, reliable.getInfo(), reliable.getStreamName())
			return retry(backoff+1, reliable)
		}
	} else {
		logs.LogError("[Reliable] - The stream %s does not exist for %s. Closing..", reliable.getStreamName(), reliable.getInfo())
		result = stream.StreamDoesNotExist
	}

	return result, true

}
