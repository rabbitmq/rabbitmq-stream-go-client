package stream

// IEntity represents the common operations shared by Producer and Consumer.
// Both *Producer and *Consumer implement this interface.
type IEntity interface {
	Close() error
	NotifyClose() ChannelClose
	GetStreamName() string
	GetName() string
	GetID() uint8
	setID(id uint8)
}
