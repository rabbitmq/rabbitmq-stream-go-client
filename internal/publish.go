package internal

import "bufio"

//   offset += WireFormatting.WriteByte(span[offset..], publisherId);
//  // this assumes we never write an empty publish frame
//  offset += WireFormatting.WriteInt32(span[offset..], MessageCount);
//  offset += msg.Write(mybytes[][offset..]); //1

//  foreach (var (publishingId, msg) in messages)
//  {
//  offset += WireFormatting.WriteUInt64(span[offset..], publishingId);
//                // this only write "simple" messages, we assume msg is just the binary body
//                // not stream encoded data
//  offset += WireFormatting.WriteUInt32(span[offset..], (uint)msg.Size);
//  offset += msg.Write(span[offset..]); //1
//  }

type PublishRequest struct {
	publisherId  uint8
	messageCount uint32
	messages     []byte //
}

func (p PublishRequest) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, p.publisherId, p.messageCount, p.messages)
}

func (p PublishRequest) Key() uint16 {
	return CommandPublish
}

func (p PublishRequest) SizeNeeded() int {
	return streamProtocolKeySizeBytes +
		streamProtocolVersionSizeBytes +
		streamProtocolKeySizeUint8 + // publisherId
		streamProtocolKeySizeUint32 + // messageCount
		len(p.messages) // messages
}

func (p PublishRequest) Version() int16 {
	return Version1
}

func NewPublishRequest(publisherId uint8, messageCount uint32, messages []byte) *PublishRequest {
	return &PublishRequest{publisherId: publisherId, messageCount: messageCount, messages: messages}
}
