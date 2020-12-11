package stream

import (
	"encoding/binary"
	"io"
)

func ReadUShort(inputBuff []byte) uint16 {
	data := binary.BigEndian.Uint16(inputBuff)
	return data
}

func ReadShortFromReader(readerStream io.Reader) int16 {
	var buff = make([]byte, 2)
	readerStream.Read(buff)
	return ReadShort(buff)
}

func ReadShort(inputBuff []byte) int16 {
	return int16(ReadUShort(inputBuff))
}

func ReadIntFromReader(readerStream io.Reader) int32 {
	var buff = make([]byte, 4)
	readerStream.Read(buff)
	return ReadInt(buff)
}

func ReadUInt(inputBuff []byte) uint32 {
	data := binary.BigEndian.Uint32(inputBuff)
	return data
}

func ReadInt(inputBuff []byte) int32 {
	return int32(ReadUInt(inputBuff))
}
