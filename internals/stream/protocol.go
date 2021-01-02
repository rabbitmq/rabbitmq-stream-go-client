package stream

type Response struct {
<<<<<<< HEAD
	FrameLen      int32
	CommandID     int16
	Key           int16
	Version       int16
	CorrelationId int32
	ResponseCode  int16
}


=======
	FrameLen          int32
	CommandID         int16
	Key               int16
	Version           int16
	CorrelationId     int32
	ResponseCode      int16
	PublishID         byte
	PublishingIdCount int32
}
>>>>>>> Add publish batch
