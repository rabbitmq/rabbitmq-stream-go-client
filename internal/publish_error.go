package internal

import (
	"bufio"
	"bytes"
)

type PublishErrorResponse struct {
	publisherId     uint8 // publisher id
	publishingError PublishingError
}

type PublishingError struct {
	publishingId uint64
	code         uint16
}

func NewPublishErrorResponse(publisherId uint8, publishingId uint64, code uint16) *PublishErrorResponse {
	return &PublishErrorResponse{
		publisherId:     publisherId,
		publishingError: PublishingError{publishingId: publishingId, code: code},
	}
}

func (p *PublishErrorResponse) Key() uint16 {
	return CommandPublishError
}

func (p *PublishErrorResponse) MinVersion() int16 {
	return Version1
}

func (p *PublishErrorResponse) MaxVersion() int16 {
	return Version1
}

func (p *PublishErrorResponse) PublisherId() uint8 {
	return p.publisherId
}

func (p *PublishErrorResponse) PublishingId() uint64 {
	return p.publishingError.publishingId
}

func (p *PublishErrorResponse) Code() uint16 {
	return p.publishingError.code
}

func (p *PublishErrorResponse) Read(rd *bufio.Reader) error {
	err := readMany(rd, &p.publisherId, &p.publishingError.publishingId, &p.publishingError.code)
	if err != nil {
		return err
	}

	return nil
}

func (p *PublishErrorResponse) MarshalBinary() (data []byte, err error) {
	buff := &bytes.Buffer{}
	_, err = writeMany(buff, p.publisherId, p.publishingError.publishingId, p.publishingError.code)
	if err != nil {
		return nil, err
	}
	data = buff.Bytes()
	return
}
