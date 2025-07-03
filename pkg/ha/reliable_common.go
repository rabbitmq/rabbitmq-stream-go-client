package ha

import (
	"errors"
	"math/rand"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
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
	GetStreamName() string
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
	logs.LogInfo("[Reliable] - The %s for the stream %s is in reconnection in %d milliseconds", reliable.getInfo(), reliable.GetStreamName(), waitTime)
	time.Sleep(time.Duration(waitTime) * time.Millisecond)
	streamMetaData, errS := reliable.getEnv().StreamMetaData(reliable.GetStreamName())
	if errors.Is(errS, stream.StreamDoesNotExist) {
		logs.LogInfo("[Reliable] - The stream %s does not exist for %s. Stopping it", reliable.GetStreamName(), reliable.getInfo())
		return errS, false
	}
	if errors.Is(errS, stream.StreamNotAvailable) {
		logs.LogInfo("[Reliable] - The stream %s is not available for %s. Trying to reconnect", reliable.GetStreamName(), reliable.getInfo())
		return retry(backoff+1, reliable)
	}
	if errors.Is(errS, stream.LeaderNotReady) {
		logs.LogInfo("[Reliable] - The leader for the stream %s is not ready for %s. Trying to reconnect", reliable.GetStreamName(), reliable.getInfo())
		return retry(backoff+1, reliable)
	}

	if errors.Is(errS, stream.StreamMetadataFailure) {
		logs.LogInfo("[Reliable] - Fail to retrieve the %s metadata for %s. Trying to reconnect", reliable.GetStreamName(), reliable.getInfo())
		return retry(backoff+1, reliable)
	}

	var result error
	if streamMetaData != nil {
		logs.LogInfo("[Reliable] - The stream %s exists. Reconnecting the %s.", reliable.GetStreamName(), reliable.getInfo())
		result = reliable.getNewInstance()()
		if result == nil {
			logs.LogInfo("[Reliable] - The stream %s exists. %s reconnected.", reliable.getInfo(), reliable.GetStreamName())
		} else {
			logs.LogInfo("[Reliable] - error %s creating %s for the stream %s. Trying to reconnect", result, reliable.getInfo(), reliable.GetStreamName())
			return retry(backoff+1, reliable)
		}
	} else {
		logs.LogError("[Reliable] - The stream %s does not exist for %s. Closing..", reliable.GetStreamName(), reliable.getInfo())
		return stream.StreamDoesNotExist, false
	}

	return result, true
}

func randomWaitWithBackoff(attempt int) int {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	baseWait := 3_000 + r.Intn(8_000)

	// Calculate the wait time considering the number of attempts
	waitTime := min(baseWait*(1<<(attempt-1)), 15_000)

	return waitTime
}
