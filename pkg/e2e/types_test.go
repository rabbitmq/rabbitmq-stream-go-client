//go:build rabbitmq.stream.e2e

package e2e_test

import (
	"encoding/binary"
	"io"
)

type plainTextMessage struct {
	body string
}

func (p *plainTextMessage) WriteTo(w io.Writer) (n int64, err error) {
	n = 0
	err = binary.Write(w, binary.BigEndian, uint32(len(p.body)))
	if err != nil {
		return
	}
	n += 2

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
