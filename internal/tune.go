package internal

import (
	"bufio"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
)

// TuneRequest is initiated by the server. It's the few commands where the request is initiated from the server
// In this case, request implements the internal.SyncCommandRead interface, as opposed to other requests.
type TuneRequest struct {
	frameMaxSize    uint32
	heartbeatPeriod uint32
}

func (t *TuneRequest) FrameMaxSize() uint32 {
	return t.frameMaxSize
}

func (t *TuneRequest) HeartbeatPeriod() uint32 {
	return t.heartbeatPeriod
}

// ResponseCode is always OK for Tune frames. Tune frames do not have a response code. This function is
// implemented to conform with the interface
func (t *TuneRequest) ResponseCode() uint16 {
	return constants.ResponseCodeOK
}

func (t *TuneRequest) Read(reader *bufio.Reader) error {
	err := readMany(reader, &t.frameMaxSize, &t.heartbeatPeriod)
	if err != nil {
		return err
	}
	return nil
}

// CorrelationId always returns 0. Tune frames do not have a correlation ID. This function is a placeholder to conform
// to the internal.SyncCommandRead interface.
func (t *TuneRequest) CorrelationId() uint32 {
	return 0
}

// TuneResponse is sent by the client. It's the few commands where the server sends a request and expects a response.
// In this case, the response implements the internal.SyncCommandWrite interface, as opposed to other responses.
type TuneResponse struct {
	TuneRequest
}

func NewTuneResponse(frameMaxSize, heartbeat uint32) *TuneResponse {
	t := &TuneResponse{}
	t.frameMaxSize = frameMaxSize
	t.heartbeatPeriod = heartbeat
	return t
}

func (t *TuneResponse) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, t.frameMaxSize, t.heartbeatPeriod)
}

func (t *TuneResponse) Key() uint16 {
	return CommandTune
}

func (t *TuneResponse) SizeNeeded() int {
	return streamProtocolKeySizeBytes +
		streamProtocolVersionSizeBytes +
		streamProtocolMaxFrameSizeBytes +
		streamProtocolHeartbeatPeriodBytes
}

// SetCorrelationId is a no-op. Tune frames do not have a correlation ID. This function is a placeholder to conform to
// the internal.SyncCommandWrite interface.
func (t *TuneResponse) SetCorrelationId(correlationId uint32) {}

func (t *TuneResponse) Version() int16 {
	return Version1
}
