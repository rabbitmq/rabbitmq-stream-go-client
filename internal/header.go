package internal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
)

type Header struct {
	// total size of this header + command content
	length int
	// Key ID
	command uint16
	version int16
}

func NewHeader(length int, command uint16, version int16) *Header {
	return &Header{length: length, command: command, version: version}
}

func (h *Header) Command() uint16 {
	return h.command
}

func (h *Header) Version() int16 {
	return h.version
}

func (h *Header) UnmarshalBinary(data []byte) error {
	reader := bytes.NewReader(data)
	var l uint32
	if err := binary.Read(reader, binary.BigEndian, &l); err != nil {
		return err
	}
	h.length = int(l)
	if err := binary.Read(reader, binary.BigEndian, &h.command); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &h.version); err != nil {
		return err
	}
	reader.Len()
	return nil
}

func (h *Header) MarshalBinary() (data []byte, err error) {
	buf := new(bytes.Buffer)
	wr := bufio.NewWriter(buf)
	n, err := writeMany(wr, h.length, h.command, h.version)
	if err != nil {
		return nil, err
	}
	if n != 8 {
		return nil, fmt.Errorf("wrote unexpected number of bytes: wrote %d, expected 8", n)
	}

	if err = wr.Flush(); err != nil {
		return nil, err
	}
	data = buf.Bytes()
	return data, nil
}

func NewHeaderRequest(command CommandWrite) *Header {
	return &Header{
		length:  command.SizeNeeded(),
		command: command.Key(),
		version: command.Version(),
	}
}

func (h *Header) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, h.length, h.command, h.version)
}

func (h *Header) Read(reader *bufio.Reader) error {
	return readMany(reader, &h.length, &h.command, &h.version)
}
