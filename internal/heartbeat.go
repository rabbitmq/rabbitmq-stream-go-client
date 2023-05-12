package internal

import "bufio"

type Heartbeat struct {
	command uint16
	version int16
}

func NewHeartbeat() *Heartbeat {
	return &Heartbeat{
		command: CommandHeartbeat,
		version: Version1,
	}
}

func (h *Heartbeat) SizeNeeded() int {
	return streamProtocolHeaderSizeBytes
}

func (h *Heartbeat) Key() uint16 {
	return CommandHeartbeat
}

func (h *Heartbeat) Version() int16 {
	return Version1
}

func (h *Heartbeat) Write(wr *bufio.Writer) (int, error) {
	return writeMany(wr, h.command, h.version)
}

func (h *Heartbeat) Read(rd *bufio.Reader) error {
	return readMany(rd, &h.command, &h.version)
}
