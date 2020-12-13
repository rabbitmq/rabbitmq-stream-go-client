package stream

type Response struct {
	FrameLen      int32
	CommandID     int16
	Key           int16
	Version       int16
	CorrelationId int32
	ResponseCode  int16
}


