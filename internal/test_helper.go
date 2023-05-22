//go:build rabbitmq.stream.test

package internal

import "bytes"

type FakeMessage struct {
	publishingId uint64
	body         []byte
}

func (f *FakeMessage) UnmarshalBinary(data []byte) error {
	reader := bytes.NewReader(data)
	var length uint32
	err := readMany(reader, &f.publishingId, &length)
	if err != nil {
		return err
	}
	f.body = make([]byte, length)
	err = readMany(reader, &f.body)
	if err != nil {
		return err
	}
	return nil
}

func (f *FakeMessage) MarshalBinary() (data []byte, err error) {
	buff := new(bytes.Buffer)
	_, err = writeMany(buff, f.publishingId, uint32(len(f.body)), f.body)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func (f *FakeMessage) SetPublishingId(publishingId uint64) {
	f.publishingId = publishingId
}

func (f *FakeMessage) GetPublishingId() uint64 {
	return f.publishingId
}

func (f *FakeMessage) SetBody(body []byte) {
	f.body = body
}

func (f *FakeMessage) Body() []byte {
	return f.body
}

func NewFakeMessage(publishingId uint64, body []byte) *FakeMessage {
	return &FakeMessage{publishingId: publishingId, body: body}
}
