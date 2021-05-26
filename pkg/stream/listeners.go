package stream

type Event struct {
	Command    uint16
	StreamName string
	Name       string
	Reason     string
	Err        error
}

type PublishError struct {
	Code               uint16
	Err                error
	UnConfirmedMessage *UnConfirmedMessage
}

type onInternalClose func(ch <-chan uint8)
type metadataListener func(ch <-chan string)

type ChannelClose = <-chan Event
type ChannelPublishError = <-chan PublishError
type ChannelPublishConfirm <-chan []*UnConfirmedMessage
