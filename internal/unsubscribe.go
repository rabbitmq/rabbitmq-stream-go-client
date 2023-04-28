package internal

import (
	"bufio"
	"bytes"
)

type UnsubscribeRequest struct {
	correlationId  uint32
	subscriptionId uint8
}

func NewUnsubscribeRequest(subscriptionId uint8) *UnsubscribeRequest {
	return &UnsubscribeRequest{subscriptionId: subscriptionId}
}

func (u *UnsubscribeRequest) Key() uint16 {
	return CommandUnsubscribe
}

func (u *UnsubscribeRequest) Version() int16 {
	return Version1
}

func (u *UnsubscribeRequest) CorrelationId() uint32 {
	return u.correlationId
}

func (u *UnsubscribeRequest) SubscriptionId() uint8 {
	return u.subscriptionId
}

func (u *UnsubscribeRequest) SetCorrelationId(id uint32) {
	u.correlationId = id
}

func (u *UnsubscribeRequest) SizeNeeded() int {
	return streamProtocolHeader + // Key Version CorrelationId
		streamProtocolKeySizeUint8 // SubscriptionId
}

func (u *UnsubscribeRequest) Write(wr *bufio.Writer) (int, error) {
	return writeMany(wr, u.correlationId, u.subscriptionId)
}

func (u *UnsubscribeRequest) UnmarshalBinary(data []byte) error {
	buff := bytes.NewReader(data)
	rd := bufio.NewReader(buff)
	return readMany(rd, &u.correlationId, &u.subscriptionId)
}
