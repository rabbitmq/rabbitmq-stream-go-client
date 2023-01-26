package internal

import (
	"bufio"
	"bytes"
	"io"
)

type ChunkResponse struct {
	SubscriptionId   uint8
	CommittedChunkId uint64
	//OsirisChunk => MagicVersion NumEntries NumRecords Epoch ChunkFirstOffset ChunkCrc DataLength Messages
	MagicVersion     int8
	ChunkType        int8 // 0: user, 1: tracking delta, 2: tracking snapshot
	NumEntries       uint16
	NumRecords       uint32
	Timestamp        int64 // erlang system time in milliseconds, since epoch
	Epoch            uint64
	ChunkFirstOffset uint64
	ChunkCrc         int32
	DataLength       uint32
	TrailerLength    uint32
	Reserved         uint32 // unused 4 bytes
	Messages         []byte // raw bytes of the messages. The protocol here defines the messages.
	// but at this level we don't parse the messages.
	//Messages[StreamerMessage]        // no int32 for the size for this array; the size is defined by NumEntries field above
	//Message  EntryTypeAndSize
	//Data => bytes
}

func (c *ChunkResponse) Key() uint16 {
	return CommandDeliver
}

func (c *ChunkResponse) MinVersion() int16 {
	// TODO: change this to version 2 after the bug in the server is fixed
	// 		https://vmware.slack.com/archives/C039S4USVPG/p1674748275224219
	return Version1
}

func (c *ChunkResponse) MaxVersion() int16 {
	return Version2
}

func (c *ChunkResponse) MarshalBinary() (data []byte, err error) {
	buff := &bytes.Buffer{}
	nn, err := writeMany(
		buff,
		c.SubscriptionId,
		c.CommittedChunkId,
		c.MagicVersion,
		c.ChunkType,
		c.NumEntries,
		c.NumRecords,
		c.Timestamp,
		c.Epoch,
		c.ChunkFirstOffset,
		c.ChunkCrc,
		c.DataLength,
		c.TrailerLength,
		c.Reserved,
		c.Messages,
	)
	if err != nil {
		return nil, err
	}
	if nn != (57 + len(c.Messages)) {
		return nil, errWriteShort
	}

	data = buff.Bytes()
	return
}

func (c *ChunkResponse) Read(reader *bufio.Reader) error {
	err := readMany(reader, &c.SubscriptionId, &c.CommittedChunkId,
		&c.MagicVersion, &c.ChunkType, &c.NumEntries,
		&c.NumRecords, &c.Timestamp, &c.Epoch, &c.ChunkFirstOffset, &c.ChunkCrc,
		&c.DataLength, &c.TrailerLength, &c.Reserved)
	if err != nil {
		return err
	}
	// read the messages buffer
	// The buffer can't be read with readMany because it's not a fixed size.
	// The size is defined by the DataLength field above.
	// io.ReadFull is a "blocking" read, it will read the exact number of bytes
	// we need it to calculate the checksum and to have the buffer to
	c.Messages = make([]byte, int(c.DataLength))
	_, err = io.ReadFull(reader, c.Messages)

	return err

}
