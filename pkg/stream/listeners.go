package stream

type Event struct {
	Command    uint16
	StreamName string
	Name       string
	Reason     string
	Err        error
}

type PublishError struct {
	PublisherId  uint8
	Name         string
	PublishingId int64
	Code         uint16
	ErrorMessage string
}

type onInternalClose func(ch <-chan uint8)
type metadataListener func(ch <-chan string)

type CloseListener = chan<- Event

type PublishErrorListener = chan<- PublishError

type PublishConfirmListener chan<- []int64
