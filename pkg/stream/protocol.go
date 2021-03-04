package stream

type StreamingResponse struct {
	FrameLen          int32
	CommandID         uint16
	Key               int16
	Version           int16
	CorrelationId     int32
	ResponseCode      uint16
	PublishID         byte
	PublishingIdCount int32
}

type WriteResponse struct {
	Code int
	Err  error
}
