package internal

import (
	"bufio"
	"bytes"
	"encoding/binary"
)

type Header struct {
	// total size of this header + command content
	length int
	// Key ID
	command uint16
	version int16
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
	if err = binary.Write(wr, binary.BigEndian, h.length); err != nil {
		return nil, err
	}
	if err = binary.Write(wr, binary.BigEndian, h.command); err != nil {
		return nil, err
	}
	if err = binary.Write(wr, binary.BigEndian, h.version); err != nil {
		return nil, err
	}
	copy(data, buf.Bytes())
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
