package stream

import (
	"bytes"
	"github.com/Azure/go-amqp"
)

type Producer struct {
	ProducerID  byte
	LikedClient *Client
}

func (producer *Producer) BatchPublish(msgs []*amqp.Message) error {
	frameHeaderLength := 2 + 2 + 1 + 4

	var msgLen int
	for _, msg := range msgs {
		r, _ := msg.MarshalBinary()
		msgLen += len(r) + 8 + 4
	}

	length := frameHeaderLength + msgLen
	var publishId byte
	publishId = producer.ProducerID
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandPublish)
	WriteShort(b, Version0)
	WriteByte(b, publishId)
	WriteInt(b, len(msgs)) //toExcluded - fromInclude

	var seq int64
	seq = 0
	for _, msg := range msgs {
		r, _ := msg.MarshalBinary()
		WriteLong(b, seq)   // sequence
		WriteInt(b, len(r)) // len
		b.Write(r)
		seq += 1
	}

	err := producer.LikedClient.writeAndFlush(b.Bytes())
	if err != nil {
		return err
	}
	producer.LikedClient.handleResponse()
	return nil
}
