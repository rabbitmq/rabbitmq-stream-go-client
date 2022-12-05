package internal

import "bufio"

// PublishConfirmResponse is a response that contains a publisher ID and a list of publishing IDs.
// Publish commands return this type of response. It is used to confirm that the publishing was successful.
// It is asynchronous and the response could contain a list of publishing IDs.
type PublishConfirmResponse struct {
	publisherID   uint8 // publisher id
	publishingIds []uint64
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
