package internal

import (
	"bufio"
)

const Version1 = int16(1)

type Header struct {
	length  int
	version int16
	Command uint16
}

func NewHeaderRequest(command CommandWrite) *Header {
	return &Header{length: command.SizeNeeded(), Command: command.GetKey(),
		version: Version1}
}

func (h *Header) Write(writer *bufio.Writer) (int, error) {
	return WriteMany(writer, h.length, h.Command, h.version)
}

func NewHeaderResponse() *Header {
	return &Header{}
}
func (h *Header) Read(reader *bufio.Reader) error {
	return ReadMany(reader, &h.length, &h.Command, &h.version)
}
