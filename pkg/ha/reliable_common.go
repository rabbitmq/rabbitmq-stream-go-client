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
	GetStatusAsString() string
}

// Retry is a function that retries the IReliable to the stream
// The first step is to set the status to reconnecting
// Then it sleeps for a random time between 2 and the timeout to avoid overlapping with other reconnecting
// Then it checks if the stream exists. During the restart the stream could be deleted
// If the stream does not exist it returns a StreamDoesNotExist error
// If the stream exists it tries to create a new instance of the IReliable

//
// The stream could be in a `StreamNotAvailable` status or the `LeaderNotReady`
// `StreamNotAvailable`  is a server side error: Stream exists but is not available for the producer and consumer
// `LeaderNotReady` is a client side error: Stream exists it is Ready but the leader is not elected yet. It is mandatory for the Producer
// In both cases it retries the reconnection

func retry(backoff int, reliable IReliable) (error, bool) {
	waitTime := randomWaitWithBackoff(backoff)
	logs.LogInfo("[Reliable] - The %s for the stream %s is in reconnection in %d milliseconds", reliable.getInfo(), reliable.getStreamName(), waitTime)
	time.Sleep(time.Duration(waitTime) * time.Millisecond)
	streamMetaData, errS := reliable.getEnv().StreamMetaData(reliable.getStreamName())
	if errors.Is(errS, stream.StreamDoesNotExist) {
		logs.LogInfo("[Reliable] - The stream %s does not exist for %s. Stopping it", reliable.getStreamName(), reliable.getInfo())
		return errS, false
	}
	if errors.Is(errS, stream.StreamNotAvailable) {
		logs.LogInfo("[Reliable] - The stream %s is not available for %s. Trying to reconnect", reliable.getStreamName(), reliable.getInfo())
		return retry(backoff+1, reliable)
	}
	if errors.Is(errS, stream.LeaderNotReady) {
		logs.LogInfo("[Reliable] - The leader for the stream %s is not ready for %s. Trying to reconnect", reliable.getStreamName(), reliable.getInfo())
		return retry(backoff+1, reliable)
	}

	var result error
	if streamMetaData != nil {
		logs.LogInfo("[Reliable] - The stream %s exists. Reconnecting the %s.", reliable.getStreamName(), reliable.getInfo())
		result = reliable.getNewInstance()()
		if result == nil {
			logs.LogInfo("[Reliable] - The stream %s exists. %s reconnected.", reliable.getInfo(), reliable.getStreamName())
		} else {
			logs.LogInfo("[Reliable] - error %s creating %s for the stream %s. Trying to reconnect", result, reliable.getInfo(), reliable.getStreamName())
			return retry(backoff+1, reliable)
		}
	} else {
		logs.LogError("[Reliable] - The stream %s does not exist for %s. Closing..", reliable.getStreamName(), reliable.getInfo())
		return stream.StreamDoesNotExist, false
	}

	return result, true

}

func randomWaitWithBackoff(attempt int) int {
	baseWait := 2_000 + rand.Intn(7_000)

	// Calculate the wait time considering the number of attempts
	waitTime := baseWait * (1 << (attempt - 1)) // Exponential back-off

	// Cap the wait time at a maximum of 20 seconds
	if waitTime > 20_000 {
		waitTime = 20_000
	}

	return waitTime

}
