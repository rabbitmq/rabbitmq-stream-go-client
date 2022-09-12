package internal

import (
	"bufio"
	"encoding/binary"
)

func ReadUShort(readerStream *bufio.Reader) uint16 {
	var res uint16
	_ = binary.Read(readerStream, binary.BigEndian, &res)
	return res
}

func ReadUInt(readerStream *bufio.Reader) (uint32, error) {
	var res uint32
	err := binary.Read(readerStream, binary.BigEndian, &res)
	return res, err
}

func peekByte(readerStream *bufio.Reader) (uint8, error) {
	res, err := readerStream.Peek(1)
	if err != nil {
		return 0, err
	}
	return res[0], nil
}

func ReadString(readerStream *bufio.Reader) string {
	lenString := ReadUShort(readerStream)
	buff := make([]byte, lenString)
	_ = binary.Read(readerStream, binary.BigEndian, &buff)
	return string(buff)
}

func UShortEncodeResponseCode(code uint16) uint16 {
	return code | 0b1000_0000_0000_0000
}

func UShortExtractResponseCode(code uint16) uint16 {
	return code & 0b0111_1111_1111_1111
}

func ReadMany(readerStream *bufio.Reader, args ...interface{}) error {
	for _, arg := range args {
		err := ReadAny(readerStream, arg)
		if err != nil {
			return err
		}
	}
	return nil
}

func ReadAny(readerStream *bufio.Reader, arg interface{}) error {

	switch arg.(type) {
	case *int:
		uInt, err := ReadUInt(readerStream)
		if err != nil {
			return err
		}
		*arg.(*int) = int(uInt)
		break
	case *string:
		*arg.(*string) = ReadString(readerStream)
		break
	default:
		err := binary.Read(readerStream, binary.BigEndian, arg)
		if err != nil {
			return err
		}

	}
	return nil
}

func WriteMany(writer *bufio.Writer, args ...interface{}) (int, error) {
	var written int

	for _, arg := range args {
		switch arg.(type) {
		case int:
			err := binary.Write(writer, binary.BigEndian, int32(arg.(int)))
			if err != nil {
				return written, err
			}
			written += binary.Size(int32(arg.(int)))
			break
		case string:
			n, err := WriteString(writer, arg.(string))
			if err != nil {
				return written, err
			}
			written += n
			break
		case map[string]string:
			for key, value := range arg.(map[string]string) {
				n, err := WriteString(writer, key)
				written += n
				n, err = WriteString(writer, value)
				written += n
				if err != nil {
					return written, err
				}
			}
		default:
			err := binary.Write(writer, binary.BigEndian, arg)
			if err != nil {
				return written, err
			}
			written += binary.Size(arg)
		}

	}
	return written, nil
}

func WriteString(writer *bufio.Writer, value string) (nn int, err error) {
	shortLen, err := WriteMany(writer, uint16(len(value)))
	if err != nil {
		return 0, err
	}
	byteLen, err := writer.Write([]byte(value))
	return byteLen + shortLen, err
}
