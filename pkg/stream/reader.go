package stream

import (
	"encoding/binary"
	"io"
)


func ReadUShortFromReader(readerStream io.Reader) uint16 {
	var res uint16
	_ = binary.Read(readerStream, binary.BigEndian, &res)
	return res
}

func ReadUIntFromReader(readerStream io.Reader) uint32 {
	var res uint32
	_ = binary.Read(readerStream, binary.BigEndian, &res)
	return res
}

func ReadInt64FromReader(readerStream io.Reader) int64 {
	var res int64
	_ = binary.Read(readerStream, binary.BigEndian, &res)
	return res
}

func ReadByteFromReader(readerStream io.Reader) uint8 {
	var res uint8
	_ = binary.Read(readerStream, binary.BigEndian, &res)
	return res

}

func ReadStringFromReader(readerStream io.Reader) string {
	lenString := ReadUShortFromReader(readerStream)
	buff := make([]byte, lenString)
	_ = binary.Read(readerStream, binary.BigEndian, &buff)
	return string(buff)
}
