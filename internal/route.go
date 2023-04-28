package internal

import (
	"bufio"
	"bytes"
)

type RouteQuery struct {
	correlationId uint32
	routingKey    string
	superStream   string
}

func NewRouteQuery(routingKey, superStream string) *RouteQuery {
	return &RouteQuery{routingKey: routingKey, superStream: superStream}
}

func (r *RouteQuery) SizeNeeded() int {
	return streamProtocolKeySizeUint16 + // key
		streamProtocolVersionSizeBytes + // version
		streamProtocolCorrelationIdSizeBytes + // correlationID
		streamProtocolStringLenSizeBytes + len(r.routingKey) + // routingKey
		streamProtocolStringLenSizeBytes + len(r.superStream) // superStream
}

func (r *RouteQuery) Key() uint16 {
	return CommandRoute
}

func (r *RouteQuery) Version() int16 {
	return Version1
}

func (r *RouteQuery) CorrelationId() uint32 {
	return r.correlationId
}

func (r *RouteQuery) SetCorrelationId(id uint32) {
	r.correlationId = id
}

func (r *RouteQuery) RoutingKey() string {
	return r.routingKey
}
func (r *RouteQuery) SuperStream() string {
	return r.superStream
}

func (r *RouteQuery) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, r.correlationId, r.routingKey, r.superStream)
}

func (r *RouteQuery) UnmarshalBinary(data []byte) error {
	buff := bytes.NewReader(data)
	rd := bufio.NewReader(buff)
	return readMany(rd, &r.correlationId, &r.routingKey, &r.superStream)
}

type RouteResponse struct {
	correlationId uint32
	responseCode  uint16
	streams       []string
}

func NewRouteResponse() *RouteResponse {
	return &RouteResponse{}
}

func NewRouteResponseWith(correlationId uint32, responseCode uint16, streams []string) *RouteResponse {
	return &RouteResponse{correlationId: correlationId, responseCode: responseCode, streams: streams}
}

func (r *RouteResponse) Read(reader *bufio.Reader) error {
	return readMany(reader, &r.correlationId, &r.responseCode, &r.streams)
}

func (r *RouteResponse) CorrelationId() uint32 {
	return r.correlationId
}
func (r *RouteResponse) ResponseCode() uint16 {
	return r.responseCode
}

func (r *RouteResponse) Streams() []string {
	return r.streams
}

func (r *RouteResponse) MarshalBinary() ([]byte, error) {
	var buff bytes.Buffer
	wr := bufio.NewWriter(&buff)
	_, err := writeMany(wr, r.correlationId, r.responseCode, r.streams)
	if err != nil {
		return nil, err
	}
	if err = wr.Flush(); err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}
