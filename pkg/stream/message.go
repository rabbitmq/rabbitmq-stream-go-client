package stream

// TODO maybe we dont need type alias
//type Message = raw.Message
//
//type message struct {
//	publishingId uint64
//	amqpMessage  amqp.Message
//}
//
//func NewMessage(publishingId uint64, amqpMessage amqp.Message) Message {
//	return &message{publishingId: publishingId, amqpMessage: amqpMessage}
//}
//
//func (m *message) WriteTo(w io.Writer) (n int64, err error) {
//	err = binary.Write(w, binary.BigEndian, m.publishingId)
//	if err != nil {
//		return 0, err
//	}
//	n += 8
//
//	bMessage, err := m.amqpMessage.MarshalBinary()
//	if err != nil {
//		return n, err
//	}
//	err = binary.Write(w, binary.BigEndian, uint32(len(bMessage)))
//	if err != nil {
//		return n, err
//	}
//	n += 4
//
//	err = binary.Write(w, binary.BigEndian, bMessage)
//	if err != nil {
//		return n, err
//	}
//	n += int64(len(bMessage))
//	return n, nil
//}
//
//func (m *message) SetPublishingId(publishingId uint64) {
//	m.publishingId = publishingId
//}
//
//func (m *message) PublishingId() uint64 {
//	return m.publishingId
//}
//
//func (m *message) SetMessage(message common.Serializer) {
//	m.amqpMessage = *message.(*amqp.Message)
//}
//
//func (m *message) Message() common.Serializer {
//	return &m.amqpMessage
//}
