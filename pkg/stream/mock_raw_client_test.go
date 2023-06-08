// Code generated by MockGen. DO NOT EDIT.
// Source: pkg/raw/client_types.go

// Package stream is a generated GoMock package.
package stream

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	common "github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/common"
	constants "github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
	raw "github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/raw"
)

// MockRawClient is a mock of Clienter interface.
type MockRawClient struct {
	ctrl     *gomock.Controller
	recorder *MockRawClientMockRecorder
}

// MockRawClientMockRecorder is the mock recorder for MockRawClient.
type MockRawClientMockRecorder struct {
	mock *MockRawClient
}

// NewMockRawClient creates a new mock instance.
func NewMockRawClient(ctrl *gomock.Controller) *MockRawClient {
	mock := &MockRawClient{ctrl: ctrl}
	mock.recorder = &MockRawClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRawClient) EXPECT() *MockRawClientMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockRawClient) Close(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockRawClientMockRecorder) Close(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockRawClient)(nil).Close), ctx)
}

// Connect mocks base method.
func (m *MockRawClient) Connect(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Connect", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Connect indicates an expected call of Connect.
func (mr *MockRawClientMockRecorder) Connect(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Connect", reflect.TypeOf((*MockRawClient)(nil).Connect), ctx)
}

// ConsumerUpdateResponse mocks base method.
func (m *MockRawClient) ConsumerUpdateResponse(ctx context.Context, correlationId uint32, responseCode, offsetType uint16, offset uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ConsumerUpdateResponse", ctx, correlationId, responseCode, offsetType, offset)
	ret0, _ := ret[0].(error)
	return ret0
}

// ConsumerUpdateResponse indicates an expected call of ConsumerUpdateResponse.
func (mr *MockRawClientMockRecorder) ConsumerUpdateResponse(ctx, correlationId, responseCode, offsetType, offset interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ConsumerUpdateResponse", reflect.TypeOf((*MockRawClient)(nil).ConsumerUpdateResponse), ctx, correlationId, responseCode, offsetType, offset)
}

// Credit mocks base method.
func (m *MockRawClient) Credit(ctx context.Context, subscriptionId uint8, credit uint16) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Credit", ctx, subscriptionId, credit)
	ret0, _ := ret[0].(error)
	return ret0
}

// Credit indicates an expected call of Credit.
func (mr *MockRawClientMockRecorder) Credit(ctx, subscriptionId, credit interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Credit", reflect.TypeOf((*MockRawClient)(nil).Credit), ctx, subscriptionId, credit)
}

// DeclarePublisher mocks base method.
func (m *MockRawClient) DeclarePublisher(ctx context.Context, publisherId uint8, publisherReference, stream string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeclarePublisher", ctx, publisherId, publisherReference, stream)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeclarePublisher indicates an expected call of DeclarePublisher.
func (mr *MockRawClientMockRecorder) DeclarePublisher(ctx, publisherId, publisherReference, stream interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeclarePublisher", reflect.TypeOf((*MockRawClient)(nil).DeclarePublisher), ctx, publisherId, publisherReference, stream)
}

// DeclareStream mocks base method.
func (m *MockRawClient) DeclareStream(ctx context.Context, stream string, configuration raw.StreamConfiguration) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeclareStream", ctx, stream, configuration)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeclareStream indicates an expected call of DeclareStream.
func (mr *MockRawClientMockRecorder) DeclareStream(ctx, stream, configuration interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeclareStream", reflect.TypeOf((*MockRawClient)(nil).DeclareStream), ctx, stream, configuration)
}

// DeletePublisher mocks base method.
func (m *MockRawClient) DeletePublisher(ctx context.Context, publisherId uint8) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeletePublisher", ctx, publisherId)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeletePublisher indicates an expected call of DeletePublisher.
func (mr *MockRawClientMockRecorder) DeletePublisher(ctx, publisherId interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeletePublisher", reflect.TypeOf((*MockRawClient)(nil).DeletePublisher), ctx, publisherId)
}

// DeleteStream mocks base method.
func (m *MockRawClient) DeleteStream(ctx context.Context, stream string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteStream", ctx, stream)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeleteStream indicates an expected call of DeleteStream.
func (mr *MockRawClientMockRecorder) DeleteStream(ctx, stream interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteStream", reflect.TypeOf((*MockRawClient)(nil).DeleteStream), ctx, stream)
}

// ExchangeCommandVersions mocks base method.
func (m *MockRawClient) ExchangeCommandVersions(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExchangeCommandVersions", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// ExchangeCommandVersions indicates an expected call of ExchangeCommandVersions.
func (mr *MockRawClientMockRecorder) ExchangeCommandVersions(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExchangeCommandVersions", reflect.TypeOf((*MockRawClient)(nil).ExchangeCommandVersions), ctx)
}

// IsOpen mocks base method.
func (m *MockRawClient) IsOpen() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsOpen")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsOpen indicates an expected call of IsOpen.
func (mr *MockRawClientMockRecorder) IsOpen() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsOpen", reflect.TypeOf((*MockRawClient)(nil).IsOpen))
}

// MetadataQuery mocks base method.
func (m *MockRawClient) MetadataQuery(ctx context.Context, stream string) (*raw.MetadataResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MetadataQuery", ctx, stream)
	ret0, _ := ret[0].(*raw.MetadataResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// MetadataQuery indicates an expected call of MetadataQuery.
func (mr *MockRawClientMockRecorder) MetadataQuery(ctx, stream interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MetadataQuery", reflect.TypeOf((*MockRawClient)(nil).MetadataQuery), ctx, stream)
}

// NotifyChunk mocks base method.
func (m *MockRawClient) NotifyChunk(c chan *raw.Chunk) <-chan *raw.Chunk {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyChunk", c)
	ret0, _ := ret[0].(<-chan *raw.Chunk)
	return ret0
}

// NotifyChunk indicates an expected call of NotifyChunk.
func (mr *MockRawClientMockRecorder) NotifyChunk(c interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyChunk", reflect.TypeOf((*MockRawClient)(nil).NotifyChunk), c)
}

// NotifyConnectionClosed mocks base method.
func (m *MockRawClient) NotifyConnectionClosed() <-chan error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyConnectionClosed")
	ret0, _ := ret[0].(<-chan error)
	return ret0
}

// NotifyConnectionClosed indicates an expected call of NotifyConnectionClosed.
func (mr *MockRawClientMockRecorder) NotifyConnectionClosed() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyConnectionClosed", reflect.TypeOf((*MockRawClient)(nil).NotifyConnectionClosed))
}

// NotifyConsumerUpdate mocks base method.
func (m *MockRawClient) NotifyConsumerUpdate() <-chan *raw.ConsumerUpdate {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyConsumerUpdate")
	ret0, _ := ret[0].(<-chan *raw.ConsumerUpdate)
	return ret0
}

// NotifyConsumerUpdate indicates an expected call of NotifyConsumerUpdate.
func (mr *MockRawClientMockRecorder) NotifyConsumerUpdate() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyConsumerUpdate", reflect.TypeOf((*MockRawClient)(nil).NotifyConsumerUpdate))
}

// NotifyCreditError mocks base method.
func (m *MockRawClient) NotifyCreditError(notification chan *raw.CreditError) <-chan *raw.CreditError {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyCreditError", notification)
	ret0, _ := ret[0].(<-chan *raw.CreditError)
	return ret0
}

// NotifyCreditError indicates an expected call of NotifyCreditError.
func (mr *MockRawClientMockRecorder) NotifyCreditError(notification interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyCreditError", reflect.TypeOf((*MockRawClient)(nil).NotifyCreditError), notification)
}

// NotifyHeartbeat mocks base method.
func (m *MockRawClient) NotifyHeartbeat() <-chan *raw.Heartbeat {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyHeartbeat")
	ret0, _ := ret[0].(<-chan *raw.Heartbeat)
	return ret0
}

// NotifyHeartbeat indicates an expected call of NotifyHeartbeat.
func (mr *MockRawClientMockRecorder) NotifyHeartbeat() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyHeartbeat", reflect.TypeOf((*MockRawClient)(nil).NotifyHeartbeat))
}

// NotifyMetadata mocks base method.
func (m *MockRawClient) NotifyMetadata() <-chan *raw.MetadataUpdate {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyMetadata")
	ret0, _ := ret[0].(<-chan *raw.MetadataUpdate)
	return ret0
}

// NotifyMetadata indicates an expected call of NotifyMetadata.
func (mr *MockRawClientMockRecorder) NotifyMetadata() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyMetadata", reflect.TypeOf((*MockRawClient)(nil).NotifyMetadata))
}

// NotifyPublish mocks base method.
func (m *MockRawClient) NotifyPublish(arg0 chan *raw.PublishConfirm) <-chan *raw.PublishConfirm {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyPublish", arg0)
	ret0, _ := ret[0].(<-chan *raw.PublishConfirm)
	return ret0
}

// NotifyPublish indicates an expected call of NotifyPublish.
func (mr *MockRawClientMockRecorder) NotifyPublish(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyPublish", reflect.TypeOf((*MockRawClient)(nil).NotifyPublish), arg0)
}

// NotifyPublishError mocks base method.
func (m *MockRawClient) NotifyPublishError() <-chan *raw.PublishError {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyPublishError")
	ret0, _ := ret[0].(<-chan *raw.PublishError)
	return ret0
}

// NotifyPublishError indicates an expected call of NotifyPublishError.
func (mr *MockRawClientMockRecorder) NotifyPublishError() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyPublishError", reflect.TypeOf((*MockRawClient)(nil).NotifyPublishError))
}

// Partitions mocks base method.
func (m *MockRawClient) Partitions(ctx context.Context, superStream string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Partitions", ctx, superStream)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Partitions indicates an expected call of Partitions.
func (mr *MockRawClientMockRecorder) Partitions(ctx, superStream interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Partitions", reflect.TypeOf((*MockRawClient)(nil).Partitions), ctx, superStream)
}

// QueryOffset mocks base method.
func (m *MockRawClient) QueryOffset(ctx context.Context, reference, stream string) (uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryOffset", ctx, reference, stream)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryOffset indicates an expected call of QueryOffset.
func (mr *MockRawClientMockRecorder) QueryOffset(ctx, reference, stream interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryOffset", reflect.TypeOf((*MockRawClient)(nil).QueryOffset), ctx, reference, stream)
}

// QueryPublisherSequence mocks base method.
func (m *MockRawClient) QueryPublisherSequence(ctx context.Context, reference, stream string) (uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "QueryPublisherSequence", ctx, reference, stream)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// QueryPublisherSequence indicates an expected call of QueryPublisherSequence.
func (mr *MockRawClientMockRecorder) QueryPublisherSequence(ctx, reference, stream interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "QueryPublisherSequence", reflect.TypeOf((*MockRawClient)(nil).QueryPublisherSequence), ctx, reference, stream)
}

// RouteQuery mocks base method.
func (m *MockRawClient) RouteQuery(ctx context.Context, routingKey, superStream string) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "RouteQuery", ctx, routingKey, superStream)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// RouteQuery indicates an expected call of RouteQuery.
func (mr *MockRawClientMockRecorder) RouteQuery(ctx, routingKey, superStream interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RouteQuery", reflect.TypeOf((*MockRawClient)(nil).RouteQuery), ctx, routingKey, superStream)
}

// Send mocks base method.
func (m *MockRawClient) Send(ctx context.Context, publisherId uint8, messages []common.PublishingMessager) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Send", ctx, publisherId, messages)
	ret0, _ := ret[0].(error)
	return ret0
}

// Send indicates an expected call of Send.
func (mr *MockRawClientMockRecorder) Send(ctx, publisherId, messages interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockRawClient)(nil).Send), ctx, publisherId, messages)
}

// SendHeartbeat mocks base method.
func (m *MockRawClient) SendHeartbeat() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendHeartbeat")
	ret0, _ := ret[0].(error)
	return ret0
}

// SendHeartbeat indicates an expected call of SendHeartbeat.
func (mr *MockRawClientMockRecorder) SendHeartbeat() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendHeartbeat", reflect.TypeOf((*MockRawClient)(nil).SendHeartbeat))
}

// SendSubEntryBatch mocks base method.
func (m *MockRawClient) SendSubEntryBatch(ctx context.Context, publisherId uint8, publishingId uint64, compress common.CompresserCodec, messages []common.Message) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendSubEntryBatch", ctx, publisherId, publishingId, compress, messages)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendSubEntryBatch indicates an expected call of SendSubEntryBatch.
func (mr *MockRawClientMockRecorder) SendSubEntryBatch(ctx, publisherId, publishingId, compress, messages interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendSubEntryBatch", reflect.TypeOf((*MockRawClient)(nil).SendSubEntryBatch), ctx, publisherId, publishingId, compress, messages)
}

// StoreOffset mocks base method.
func (m *MockRawClient) StoreOffset(ctx context.Context, reference, stream string, offset uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StoreOffset", ctx, reference, stream, offset)
	ret0, _ := ret[0].(error)
	return ret0
}

// StoreOffset indicates an expected call of StoreOffset.
func (mr *MockRawClientMockRecorder) StoreOffset(ctx, reference, stream, offset interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StoreOffset", reflect.TypeOf((*MockRawClient)(nil).StoreOffset), ctx, reference, stream, offset)
}

// StreamStats mocks base method.
func (m *MockRawClient) StreamStats(ctx context.Context, stream string) (map[string]int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StreamStats", ctx, stream)
	ret0, _ := ret[0].(map[string]int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StreamStats indicates an expected call of StreamStats.
func (mr *MockRawClientMockRecorder) StreamStats(ctx, stream interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StreamStats", reflect.TypeOf((*MockRawClient)(nil).StreamStats), ctx, stream)
}

// Subscribe mocks base method.
func (m *MockRawClient) Subscribe(ctx context.Context, stream string, offsetType uint16, subscriptionId uint8, credit uint16, properties constants.SubscribeProperties, offset uint64) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Subscribe", ctx, stream, offsetType, subscriptionId, credit, properties, offset)
	ret0, _ := ret[0].(error)
	return ret0
}

// Subscribe indicates an expected call of Subscribe.
func (mr *MockRawClientMockRecorder) Subscribe(ctx, stream, offsetType, subscriptionId, credit, properties, offset interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Subscribe", reflect.TypeOf((*MockRawClient)(nil).Subscribe), ctx, stream, offsetType, subscriptionId, credit, properties, offset)
}

// Unsubscribe mocks base method.
func (m *MockRawClient) Unsubscribe(ctx context.Context, subscriptionId uint8) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Unsubscribe", ctx, subscriptionId)
	ret0, _ := ret[0].(error)
	return ret0
}

// Unsubscribe indicates an expected call of Unsubscribe.
func (mr *MockRawClientMockRecorder) Unsubscribe(ctx, subscriptionId interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Unsubscribe", reflect.TypeOf((*MockRawClient)(nil).Unsubscribe), ctx, subscriptionId)
}
