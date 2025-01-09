package stream

type Event struct {
	Command    uint16
	StreamName string
	Name       string
	Reason     string
	Err        error
}

type onInternalClose func(ch <-chan uint8)

type ChannelClose = <-chan Event
type ChannelPublishConfirm chan []*ConfirmationStatus
