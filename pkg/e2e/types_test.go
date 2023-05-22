//go:build rabbitmq.stream.e2e

package e2e_test

import (
	"bytes"
	"encoding/binary"
)

type plainTextMessage struct {
	body string
}

func (p *plainTextMessage) UnmarshalBinary(data []byte) error {
	var dataLen uint32
	err := binary.Read(bytes.NewReader(data), binary.BigEndian, &dataLen)
	if err != nil {
		return err
	}

	p.body = string(data[4 : dataLen+4])

	return nil
}

func (p *plainTextMessage) MarshalBinary() ([]byte, error) {
	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.BigEndian, uint32(len(p.body)))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buff, binary.BigEndian, []byte(p.body))
	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

func (p *plainTextMessage) Body() string {
	return p.body
}
