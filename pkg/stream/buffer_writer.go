package stream

import (
	"bytes"
	"encoding/binary"
)

func WriteLong(inputBuff *bytes.Buffer, value int64) {
	WriteULong(inputBuff, uint64(value))
}

func WriteULong(inputBuff *bytes.Buffer, value uint64) {
	var buff = make([]byte, 8)
	binary.BigEndian.PutUint64(buff, value)
	inputBuff.Write(buff)
}

func WriteShort(inputBuff *bytes.Buffer, value int16) {
	WriteUShort(inputBuff, uint16(value))
}

func WriteUShort(inputBuff *bytes.Buffer, value uint16) {
	var buff = make([]byte, 2)
	binary.BigEndian.PutUint16(buff, value)
	inputBuff.Write(buff)
}

func WriteByte(inputBuff *bytes.Buffer, value byte) {
	var buff = make([]byte, 1)
	buff[0] = value
	inputBuff.Write(buff)
}

func WriteInt(inputBuff *bytes.Buffer, value int) {
	WriteUInt(inputBuff, uint32(value))
}
func WriteUInt(inputBuff *bytes.Buffer, value uint32) {
	var buff = make([]byte, 4)
	binary.BigEndian.PutUint32(buff, value)
	inputBuff.Write(buff)
}

func WriteString(inputBuff *bytes.Buffer, value string) {
	WriteUShort(inputBuff, uint16(len(value)))
	inputBuff.Write([]byte(value))
}
