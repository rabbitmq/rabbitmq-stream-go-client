package internal

import (
	"bufio"
	"bytes"
	"fmt"
)

type PublishErrorResponse struct {
	publisherId      uint8
	publishingErrors []PublishingError
}

type PublishingError struct {
	publishingId uint64
	code         uint16
}

func NewPublishingError(pubId uint64, code uint16) *PublishingError {
	return &PublishingError{publishingId: pubId, code: code}
}

func (p *PublishingError) Error() string {
	return fmt.Sprintf("error publishing message: publishingId: %d code: %#x", p.publishingId, p.code)
}

func (p *PublishingError) PublishingId() uint64 {
	return p.publishingId
}

func (p *PublishingError) Code() uint16 {
	return p.code
}

func NewPublishErrorResponse(publisherId uint8, publishingErrors ...PublishingError) *PublishErrorResponse {
	return &PublishErrorResponse{
		publisherId:      publisherId,
		publishingErrors: publishingErrors,
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

func (p *PublishErrorResponse) PublishErrors() []PublishingError {
	return p.publishingErrors
}

func (p *PublishErrorResponse) Read(rd *bufio.Reader) error {
	var errCount uint32
	err := readMany(rd, &p.publisherId, &errCount)
	if err != nil {
		return err
	}

	for i := uint32(0); i < errCount; i++ {
		var (
			publishingId uint64
			errCode      uint16
		)
		err := readMany(rd, &publishingId, &errCode)
		if err != nil {
			return err
		}
		p.publishingErrors = append(p.publishingErrors, PublishingError{publishingId, errCode})
	}

	return nil
}

func (p *PublishErrorResponse) MarshalBinary() (data []byte, err error) {
	buff := &bytes.Buffer{}
	_, err = writeMany(buff, p.publisherId, len(p.publishingErrors))
	if err != nil {
		return nil, err
	}

	for _, publishingError := range p.publishingErrors {
		_, err = writeMany(buff, publishingError.publishingId, publishingError.code)
	}

	data = buff.Bytes()
	return
}
