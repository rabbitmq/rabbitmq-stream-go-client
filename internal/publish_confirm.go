package internal

import (
	"bufio"
	"bytes"
)

// PublishConfirmResponse is a response that contains a publisher ID and a list of publishing IDs.
// Publish commands return this type of response. It is used to confirm that the publishing was successful.
// It is asynchronous and the response could contain a list of publishing IDs.
type PublishConfirmResponse struct {
	publisherID   uint8 // publisher id
	publishingIds []uint64
}

func (p *PublishConfirmResponse) Key() uint16 {
	return CommandPublishConfirm
}

func (p *PublishConfirmResponse) MinVersion() int16 {
	return Version1
}

func (p *PublishConfirmResponse) MaxVersion() int16 {
	return Version1
}

func NewPublishConfirmResponse(publisherID uint8, publishingIds []uint64) *PublishConfirmResponse {
	return &PublishConfirmResponse{publisherID: publisherID, publishingIds: publishingIds}
}

func (p *PublishConfirmResponse) PublisherID() uint8 {
	return p.publisherID
}

func (p *PublishConfirmResponse) PublishingIds() []uint64 {
	return p.publishingIds
}

func (p *PublishConfirmResponse) MarshalBinary() (data []byte, err error) {
	buff := &bytes.Buffer{}
	_, err = writeMany(buff, p.publisherID, uint32(len(p.publishingIds)), p.publishingIds)
	if err != nil {
		return nil, err
	}
	data = buff.Bytes()
	return
}

func (p *PublishConfirmResponse) Read(reader *bufio.Reader) error {
	var publishingIdCount uint32
	err := readMany(reader, &p.publisherID, &publishingIdCount)
	if err != nil {
		return err
	}
	p.publishingIds = make([]uint64, publishingIdCount)
	for i := uint32(0); i < publishingIdCount; i++ {
		err = readMany(reader, &p.publishingIds[i])
		if err != nil {
			return err
		}
	}
	return nil
}
