package internal

import "bufio"

type PublishErrorResponse struct {
	publisherId      uint8 // publisher id
	publishingErrors []PublishingError
}

type PublishingError struct {
	publishingId uint64
	code         uint16
}

func NewPublishErrorResponse(publisherId uint8, publishingErrors []PublishingError) *PublishErrorResponse {
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

func (p *PublishErrorResponse) PublishingErrors() []PublishingError {
	return p.publishingErrors
}

func (p *PublishErrorResponse) Read(rd *bufio.Reader) error {
	var publishingErrorsCount uint32
	err := readMany(rd, &p.publisherId, &publishingErrorsCount)
	if err != nil {
		return err
	}

	p.publishingErrors = make([]PublishingError, publishingErrorsCount)
	for i := uint32(0); i < publishingErrorsCount; i++ {
		err = readMany(rd, &p.publishingErrors[i].publishingId, &p.publishingErrors[i].code)
		if err != nil {
			return err
		}
	}
	return nil
}
