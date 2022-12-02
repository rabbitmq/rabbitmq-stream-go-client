package internal

import "bufio"

type PublishConfirmResponse struct {
	publishID         uint8
	publishingIdCount uint32
	publishingIds     []uint64
}

func (p *PublishConfirmResponse) Read(reader *bufio.Reader) error {
	err := readMany(reader, &p.publishID, &p.publishingIdCount)
	if err != nil {
		return err
	}
	p.publishingIds = make([]uint64, p.publishingIdCount)
	for i := uint32(0); i < p.publishingIdCount; i++ {
		err = readMany(reader, &p.publishingIds[i])
		if err != nil {
			return err
		}
	}
	return nil
}
