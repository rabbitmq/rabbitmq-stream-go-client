//go:build rabbitmq.stream.test

package internal

import (
	"io"
)

type FakeMessage struct {
	publishingId uint64
	body         []byte
}

func (f *FakeMessage) WriteTo(writer io.Writer) (int64, error) {
	n, err := writeMany(writer, f.publishingId, len(f.body), f.body)
	return int64(n), err
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
