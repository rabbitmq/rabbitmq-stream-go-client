//go:build rabbitmq.stream.e2e

package e2e_test

import (
	"bytes"
	"encoding/binary"
	"io"
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

func (p *plainTextMessage) WriteTo(w io.Writer) (n int64, err error) {
	n = 0
	err = binary.Write(w, binary.BigEndian, uint32(len(p.body)))
	if err != nil {
		return
	}
	n += 4

	n32, err := w.Write([]byte(p.body))
	n += int64(n32)
	return
}

func (p *plainTextMessage) SetBody(body []byte) {
	p.body = string(body)
}

func (p *plainTextMessage) Body() []byte {
	return []byte(p.body)
}
