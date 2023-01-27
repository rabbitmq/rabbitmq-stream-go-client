package internal

import (
	"bufio"
	"bytes"
)

type CreditRequest struct {
	subscriptionId uint8
	// number of chunks that can be sent
	credit uint16
}

func NewCreditRequest(subscriptionId uint8, credit uint16) *CreditRequest {
	return &CreditRequest{subscriptionId: subscriptionId, credit: credit}
}

func (c *CreditRequest) UnmarshalBinary(data []byte) error {
	return readMany(bytes.NewReader(data), &c.subscriptionId, &c.credit)
}

func (c *CreditRequest) SubscriptionId() uint8 {
	return c.subscriptionId
}

func (c *CreditRequest) Credit() uint16 {
	return c.credit
}

func (c *CreditRequest) Write(w *bufio.Writer) (int, error) {
	return writeMany(w, c.subscriptionId, c.credit)
}

func (c *CreditRequest) Key() uint16 {
	return CommandCredit
}

func (c *CreditRequest) SizeNeeded() int {
	return streamProtocolHeaderSizeBytes + streamProtocolKeySizeUint8 + streamProtocolKeySizeUint16
}

func (c *CreditRequest) Version() int16 {
	return Version1
}

type CreditResponse struct {
	responseCode   uint16
	subscriptionId uint8
}

func (c *CreditResponse) ResponseCode() uint16 {
	return c.responseCode
}

func (c *CreditResponse) SubscriptionId() uint8 {
	return c.subscriptionId
}

func (c *CreditResponse) MarshalBinary() (data []byte, err error) {
	w := &bytes.Buffer{}
	n, err := writeMany(w, c.responseCode, c.subscriptionId)
	if err != nil {
		return nil, err
	}

	if n != 3 {
		return nil, errWriteShort
	}

	data = w.Bytes()
	return
}

func NewCreditResponse(responseCode uint16, subscriptionId uint8) *CreditResponse {
	return &CreditResponse{responseCode: responseCode, subscriptionId: subscriptionId}
}

func (c *CreditResponse) Read(r *bufio.Reader) error {
	return readMany(r, &c.responseCode, &c.subscriptionId)
}
