package raw

import (
	"fmt"
)

type DeclarePublisherError struct {
	publisherId        uint8
	publisherReference string
	stream             string
	err                error
}

func (d *DeclarePublisherError) Error() string {
	return fmt.Sprintf("error declaring publisher, publisherId: %d, publisherReference: %s, stream: %s, error: %s",
		d.publisherId,
		d.publisherReference,
		d.stream,
		d.err,
	)
}

func (d *DeclarePublisherError) Unwrap() error {
	return d.err
}

type DeclareStreamError struct {
	stream        string
	configuration StreamConfiguration
	err           error
}

func (d *DeclareStreamError) Error() string {
	return fmt.Sprintf("error declaring stream: %s, configuration: %v, error: %s",
		d.stream,
		d.configuration,
		d.err,
	)
}

func (d *DeclareStreamError) Unwrap() error {
	return d.err
}

type DeletePublisherError struct {
	publisherId uint8
	err         error
}

func (d *DeletePublisherError) Error() string {
	return fmt.Sprintf("error deleting publisher, publisherId: %d, error: %s",
		d.publisherId,
		d.err,
	)
}

func (d *DeletePublisherError) Unwrap() error {
	return d.err
}

type DeleteStreamError struct {
	stream string
	err    error
}

func (d *DeleteStreamError) Error() string {
	return fmt.Sprintf("error deleting stream: %s, error: %s", d.stream, d.err)
}

func (d *DeleteStreamError) Unwrap() error {
	return d.err
}

type MetadataQueryError struct {
	streams []string
	err     error
}

func (m *MetadataQueryError) Error() string {
	return fmt.Sprintf("error getting metadata, stream: %s, error: %s",
		m.streams,
		m.err,
	)
}

func (m *MetadataQueryError) Unwrap() error {
	return m.err
}

type QueryOffsetError struct {
	reference string
	stream    string
	err       error
}

func (q *QueryOffsetError) Error() string {
	return fmt.Sprintf("error sending sync request to query offset, reference: %s, stream: %s, error: %s",
		q.reference,
		q.stream,
		q.err,
	)
}

func (q *QueryOffsetError) Unwarp() error {
	return q.err
}

type StreamStatsError struct {
	stream string
	err    error
}

func (s *StreamStatsError) Error() string {
	return fmt.Sprintf("error sending sync request for stream stats, stream: %s, error: %s",
		s.stream,
		s.err,
	)
}

func (s *StreamStatsError) Unwrap() error {
	return s.err
}

type SubscribeError struct {
	subscriptionId uint8
	stream         string
	offsetType     uint16
	offset         uint64
	credit         uint16
	properties     SubscribeProperties
	err            error
}

func (s *SubscribeError) Error() string {
	return fmt.Sprintf("error subscribing to consumer subscriptionId: %d, stream: %s, offsetType: %d, offset: %d, credit: %d, properties: %v, error: %s",
		s.subscriptionId,
		s.stream,
		s.offsetType,
		s.offset,
		s.credit,
		s.properties,
		s.err,
	)
}

func (s *SubscribeError) Unwrap() error {
	return s.err
}
