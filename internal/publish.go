package internal

import "bufio"

type PublishRequest struct {
	publisherId  uint8
	messageCount uint32
	messages     []byte
}

func (p *PublishRequest) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, p.publisherId, p.messageCount, p.messages)
}

func (p *PublishRequest) Key() uint16 {
	return CommandPublish
}

func (p *PublishRequest) SizeNeeded() int {
	return streamProtocolKeySizeBytes +
		streamProtocolVersionSizeBytes +
		streamProtocolKeySizeUint8 + // publisherId
		streamProtocolKeySizeUint32 + // messageCount
		len(p.messages) // messages
}

func (p *PublishRequest) Version() int16 {
	return Version1
}

func NewPublishRequest(publisherId uint8, messageCount uint32, messages []byte) *PublishRequest {
	return &PublishRequest{publisherId: publisherId, messageCount: messageCount, messages: messages}
}
