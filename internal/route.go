package internal

import "bufio"

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
