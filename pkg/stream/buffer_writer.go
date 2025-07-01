package stream

import (
	"bufio"
	"bytes"
	"encoding/binary"
)

func writeLong(inputBuff *bytes.Buffer, value int64) {
	writeULong(inputBuff, uint64(value))
}

func writeULong(inputBuff *bytes.Buffer, value uint64) {
	var buff = make([]byte, 8)
	binary.BigEndian.PutUint64(buff, value)
	inputBuff.Write(buff)
}

func writeBLong(inputBuff *bufio.Writer, value int64) error {
	return writeBULong(inputBuff, uint64(value))
}

func writeBULong(inputBuff *bufio.Writer, value uint64) error {
	var buff = make([]byte, 8)
	binary.BigEndian.PutUint64(buff, value)
	_, err := inputBuff.Write(buff)
	return err
}

func writeShort(inputBuff *bytes.Buffer, value int16) {
	writeUShort(inputBuff, uint16(value))
}

func writeUShort(inputBuff *bytes.Buffer, value uint16) {
	var buff = make([]byte, 2)
	binary.BigEndian.PutUint16(buff, value)
	inputBuff.Write(buff)
}

func writeBShort(inputBuff *bufio.Writer, value int16) error {
	return writeBUShort(inputBuff, uint16(value))
}
func writeBUShort(inputBuff *bufio.Writer, value uint16) error {
	var buff = make([]byte, 2)
	binary.BigEndian.PutUint16(buff, value)
	_, err := inputBuff.Write(buff)
	return err
}

func writeBString(inputBuff *bufio.Writer, value string) error {
	err := writeBUShort(inputBuff, uint16(len(value)))
	if err != nil {
		return err
	}

	_, err = inputBuff.Write([]byte(value))
	return err
}

func writeByte(inputBuff *bytes.Buffer, value byte) {
	var buff = make([]byte, 1)
	buff[0] = value
	inputBuff.Write(buff)
}

func writeBByte(inputBuff *bufio.Writer, value byte) error {
	var buff = make([]byte, 1)
	buff[0] = value
	_, err := inputBuff.Write(buff)
	return err
}

func writeInt(inputBuff *bytes.Buffer, value int) {
	writeUInt(inputBuff, uint32(value))
}
func writeUInt(inputBuff *bytes.Buffer, value uint32) {
	var buff = make([]byte, 4)
	binary.BigEndian.PutUint32(buff, value)
	inputBuff.Write(buff)
}

func writeBInt(inputBuff *bufio.Writer, value int) error {
	return writeBUInt(inputBuff, uint32(value))
}

func writeBUInt(inputBuff *bufio.Writer, value uint32) error {
	var buff = make([]byte, 4)
	binary.BigEndian.PutUint32(buff, value)
	_, err := inputBuff.Write(buff)
	return err
}

func writeString(inputBuff *bytes.Buffer, value string) {
	writeUShort(inputBuff, uint16(len(value)))
	inputBuff.Write([]byte(value))
}

func writeStringArray(inputBuff *bytes.Buffer, array []string) {
	writeInt(inputBuff, len(array))
	for _, s := range array {
		writeString(inputBuff, s)
	}
}

func writeMapStringString(inputBuff *bytes.Buffer, mapString map[string]string) {
	writeInt(inputBuff, len(mapString))
	for k, v := range mapString {
		writeString(inputBuff, k)
		writeString(inputBuff, v)
	}
}

func writeBytes(inputBuff *bytes.Buffer, value []byte) {
	inputBuff.Write(value)
}

// writeProtocolHeader protocol utils functions
func writeProtocolHeader(inputBuff *bytes.Buffer,
	length int, command uint16,
	correlationId ...int) {
	writeInt(inputBuff, length)
	writeUShort(inputBuff, command)
	writeShort(inputBuff, version1)
	if len(correlationId) > 0 {
		writeInt(inputBuff, correlationId[0])
	}
}

func writeBProtocolHeader(inputBuff *bufio.Writer,
	length int, command int16,
	correlationId ...int) error {
	return writeBProtocolHeaderVersion(inputBuff, length, command, version1, correlationId...)
}

func writeBProtocolHeaderVersion(inputBuff *bufio.Writer, length int, command int16,
	version int16, correlationId ...int) error {
	if err := writeBInt(inputBuff, length); err != nil {
		return err
	}
	if err := writeBShort(inputBuff, command); err != nil {
		return err
	}
	if err := writeBShort(inputBuff, version); err != nil {
		return err
	}

	if len(correlationId) > 0 {
		if err := writeBInt(inputBuff, correlationId[0]); err != nil {
			return err
		}
	}

	return nil
}

func sizeOfStringArray(array []string) int {
	size := 0
	for _, s := range array {
		size += 2 + len(s)
	}
	return size
}

func sizeOfMapStringString(mapString map[string]string) int {
	size := 0
	for k, v := range mapString {
		size += 2 + len(k) + 2 + len(v)
	}
	return size
}

func bytesLenghPrefixed(msg []byte) []byte {
	size := len(msg)
	buff := make([]byte, 4+size)

	binary.BigEndian.PutUint32(buff, uint32(size))
	copy(buff[4:], msg)

	return buff
}
