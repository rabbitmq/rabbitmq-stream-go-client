package stream

import (
	"bytes"
)

type Consumer struct {
}

func (client *Client) NewConsumer(stream string) (*Consumer, error) {
	return client.declareConsumer(stream)
}

func (client *Client) declareConsumer(stream string) (*Consumer, error) {

	length := 2 + 2 + 4 + 1 + 2 + len(stream) + 2 + 2 // misses the offset
	//if (offsetSpecification.isOffset() || offsetSpecification.isTimestamp()) {
	//	length += 8;
	//}
	resp := client.responses.New()
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))

	WriteInt(b, length)
	WriteUShort(b, CommandSubscribe)
	WriteUShort(b, Version1)
	WriteInt(b, correlationId)
	WriteByte(b, 1)

	WriteString(b, stream)

	WriteShort(b, 1)

	//	if (offsetSpecification.isOffset() || offsetSpecification.isTimestamp()) {
	//	WriteLong(offsetSpecification.getOffset());
	//}
	WriteShort(b, 10)

	client.writeAndFlush(b.Bytes())

	<-resp.code
	err := client.responses.RemoveById(correlationId)
	if err != nil {
		return nil, err
	}
	return nil, nil
}


