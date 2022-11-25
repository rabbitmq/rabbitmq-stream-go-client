//go:build rabbitmq.stream.test

package internal

import (
	"bufio"
)

type FakeMessage struct {
	publishingId uint64
	body         []byte
}

func (f *FakeMessage) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, f.publishingId, len(f.body), f.body)

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

func (f *FakeMessage) GetBody() []byte {
	return f.body
}

func NewFakeMessage(publishingId uint64, body []byte) *FakeMessage {
	return &FakeMessage{publishingId: publishingId, body: body}
}
