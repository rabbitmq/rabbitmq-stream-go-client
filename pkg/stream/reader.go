package stream

import (
	"encoding/binary"
	"fmt"
	"io"
)

func ReadUShort(inputBuff []byte) uint16 {
	data := binary.BigEndian.Uint16(inputBuff)
	return data
}

func ReadUShortFromReader(readerStream io.Reader) uint16 {
	var buff = make([]byte, 2)
	readerStream.Read(buff)
	return ReadUShort(buff)
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
	v, err := readerStream.Read(buff)
	if err != nil {
		fmt.Printf("err %s v %d", err, v)
	}
	return ReadInt(buff)
}

func ReadUIntFromReader(readerStream io.Reader) uint32 {
	var buff = make([]byte, 4)
	readerStream.Read(buff)
	return ReadUInt(buff)
}

func ReadInt64FromReader(readerStream io.Reader) int64 {
	var buff = make([]byte, 8)
	readerStream.Read(buff)
	return ReadInt64(buff)
}

func ReadByteFromReader(readerStream io.Reader) byte {
	var buff = make([]byte, 1)
	readerStream.Read(buff)
	return buff[0]
}

func ReadUInt(inputBuff []byte) uint32 {
	data := binary.BigEndian.Uint32(inputBuff)
	return data
}

func ReadUInt64(inputBuff []byte) uint64 {
	data := binary.BigEndian.Uint64(inputBuff)
	return data
}

func ReadInt(inputBuff []byte) int32 {
	return int32(ReadUInt(inputBuff))
}
func ReadInt64(inputBuff []byte) int64 {
	return int64(ReadUInt64(inputBuff))
}

func ReadStringFromReader(readerStream io.Reader) string {
	var buff = make([]byte, 2)
	readerStream.Read(buff)
	lenString := ReadShort(buff)
	buff = make([]byte, lenString)
	readerStream.Read(buff)
	return string(buff)
}
