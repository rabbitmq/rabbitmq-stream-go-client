package internal

import (
	"bufio"
	"bytes"
)

type Unsubscribe struct {
	correlationId  uint32
	subscriptionId uint8
}

func NewUnsubscribe(subscriptionId uint8) *Unsubscribe {
	return &Unsubscribe{subscriptionId: subscriptionId}
}

func (u *Unsubscribe) Key() uint16 {
	return CommandUnsubscribe
}

func (u *Unsubscribe) Version() int16 {
	return Version1
}

func (u *Unsubscribe) CorrelationId() uint32 {
	return u.correlationId
}

func (u *Unsubscribe) SubscriptionId() uint8 {
	return u.subscriptionId
}

func (u *Unsubscribe) SetCorrelationId(id uint32) {
	u.correlationId = id
}

func (u *Unsubscribe) SizeNeeded() int {
	return streamProtocolHeader + // Key Version CorrelationId
		streamProtocolKeySizeUint8 // SubscriptionId
}

func (u *Unsubscribe) Write(wr *bufio.Writer) (int, error) {
	return writeMany(wr, u.correlationId, u.subscriptionId)
}

func (u *Unsubscribe) UnmarshalBinary(data []byte) error {
	buff := bytes.NewReader(data)
	rd := bufio.NewReader(buff)
	return readMany(rd, &u.correlationId, &u.subscriptionId)
}
