package internal

import (
	"encoding/binary"
	"io"
)

func readUShort(readerStream io.Reader) (uint16, error) {
	var res uint16
	err := binary.Read(readerStream, binary.BigEndian, &res)
	return res, err
}

func readUInt(readerStream io.Reader) (uint32, error) {
	var res uint32
	err := binary.Read(readerStream, binary.BigEndian, &res)
	return res, err
}

func readString(readerStream io.Reader) string {
	// FIXME: handle the potential error from readUShort
	lenString, _ := readUShort(readerStream)
	buff := make([]byte, lenString)
	_ = binary.Read(readerStream, binary.BigEndian, &buff)
	return string(buff)
}

func readByteSlice(readerStream io.Reader) (data []byte, err error) {
	numEntries, err := readUShort(readerStream)
	if err != nil {
		return nil, err
	}
	data = make([]byte, numEntries)
	err = binary.Read(readerStream, binary.BigEndian, data)
	if err != nil {
		return nil, err
	}
	return
}

func EncodeResponseCode(code uint16) uint16 {
	return code | 0b1000_0000_0000_0000
}

func ExtractCommandCode(code uint16) uint16 {
	return code & 0b0111_1111_1111_1111
}

func readMany(readerStream io.Reader, args ...interface{}) error {
	for _, arg := range args {
		err := readAny(readerStream, arg)
		if err != nil {
			return err
		}
	}
	return nil
}

func readAny(readerStream io.Reader, arg interface{}) error {

	switch arg := arg.(type) {
	case *int:
		uInt, err := readUInt(readerStream)
		if err != nil {
			return err
		}
		*arg = int(uInt)
	case *string:
		*arg = readString(readerStream)
	case *[]byte:
		byteSlice, err := readByteSlice(readerStream)
		if err != nil {
			return err
		}
		*arg = byteSlice
	case *map[string]string:
		mapLen, err := readUInt(readerStream)
		if err != nil {
			return err
		}
		myMap := make(map[string]string, mapLen)
		for i := uint32(0); i < mapLen; i++ {
			k := readString(readerStream)
			v := readString(readerStream)
			myMap[k] = v
		}
		*arg = myMap
	default:
		err := binary.Read(readerStream, binary.BigEndian, arg)
		if err != nil {
			return err
		}

	}
	return nil
}
func WriteMany(writer io.Writer, args ...any) (int, error) {
	return writeMany(writer, args...)
}

func writeMany(writer io.Writer, args ...any) (int, error) {
	var written int

	for _, arg := range args {
		switch arg := arg.(type) {
		case int:
			err := binary.Write(writer, binary.BigEndian, int32(arg))
			if err != nil {
				return written, err
			}
			written += binary.Size(int32(arg))
		case string:
			n, err := writeString(writer, arg)
			if err != nil {
				return written, err
			}
			written += n
		case map[string]string:
			n, err := writeMany(writer, len(arg))
			if err != nil {
				return n, err
			}
			written += n
			for key, value := range arg {
				n, err := writeString(writer, key)
				written += n
				if err != nil {
					return n, err
				}
				n, err = writeString(writer, value)
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

func writeString(writer io.Writer, value string) (nn int, err error) {
	shortLen, err := writeMany(writer, uint16(len(value)))
	if err != nil {
		return 0, err
	}
	byteLen, err := writer.Write([]byte(value))
	return byteLen + shortLen, err
}
