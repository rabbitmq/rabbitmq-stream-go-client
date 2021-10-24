package stream

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"io"
)

const (
	None = byte(0)
	GZIP = byte(1)
	// Not implemented yet
	//SNAPPY = byte(2)
	//LZ4    = byte(3)
	//ZSTD   = byte(4)
)

type Compression struct {
	enabled bool
	value   byte
}

func (compression Compression) None() Compression {
	return Compression{value: None, enabled: true}
}

func (compression Compression) Gzip() Compression {
	return Compression{value: GZIP, enabled: true}
}

// Not implemented yet
//func (compression Compression) Snappy() Compression {
//	return Compression{value: SNAPPY}
//}
//func (compression Compression) Lz4() Compression {
//	return Compression{value: LZ4}
//}
//func (compression Compression) Zstd() Compression {
//	return Compression{value: ZSTD}
//}

// TODO:
//gizp tests
//documentation
//refactor stardad publish
//mak the other cmpress as not implemented yet

type subEntry struct {
	messages     []messageSequence
	publishingId int64 // need to store the publishingId useful in case of aggregation

	unCompressedSize int
	sizeInBytes      int
	dataInBytes      []byte
}
type subEntries struct {
	items            []*subEntry
	totalSizeInBytes int
}

type iCompress interface {
	Compress(subEntries *subEntries)
	UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) *bufio.Reader
}

func compressByValue(value byte) iCompress {

	switch value {
	case GZIP:
		return compressGZIP{}

	}

	return compressNONE{}
}

type compressNONE struct {
}

func (es compressNONE) Compress(subEntries *subEntries) {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		for _, msg := range entry.messages {
			writeInt(&tmp, len(msg.messageBytes))
			tmp.Write(msg.messageBytes)
		}
		entry.dataInBytes = tmp.Bytes()
		entry.sizeInBytes += len(entry.dataInBytes)
		subEntries.totalSizeInBytes += len(entry.dataInBytes)
	}
}

func (es compressNONE) UnCompress(source *bufio.Reader, _, _ uint32) *bufio.Reader {
	return source
}

type compressGZIP struct {
}

func (es compressGZIP) Compress(subEntries *subEntries) {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		w := gzip.NewWriter(&tmp)
		for _, msg := range entry.messages {
			size := len(msg.messageBytes)
			//w.Write(bytesFromInt((uint32(size) >> 24) & 0xFF))
			//w.Write(bytesFromInt((uint32(size) >> 16) & 0xFF))
			//w.Write(bytesFromInt((uint32(size) >> 8) & 0xFF))
			//w.Write(bytesFromInt((uint32(size) >> 0) & 0xFF))
			w.Write(bytesFromInt(uint32(size)))
			w.Write(msg.messageBytes)
		}
		w.Flush()
		w.Close()
		entry.sizeInBytes += len(tmp.Bytes())
		entry.dataInBytes = tmp.Bytes()
		subEntries.totalSizeInBytes += len(tmp.Bytes())
	}
}

func (es compressGZIP) UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) *bufio.Reader {

	var zipperBuffer = make([]byte, dataSize)
	_, err := source.Read(zipperBuffer)
	if err != nil {
		logs.LogError("GZIP Error during reading buffer %s", err)
	}
	reader, _ := gzip.NewReader(bytes.NewBuffer(zipperBuffer))

	// Empty byte slice.
	result := make([]byte, uncompressedDataSize)

	// Read in data.
	count, err := io.ReadFull(reader, result)
	if err != nil {
		logs.LogError("Error during reading buffer %s", err)
	}
	if uint32(count) != uncompressedDataSize {
		panic("uncompressedDataSize != count")
	}
	reader.Close()
	uncompressedReader := bytes.NewReader(result)
	return bufio.NewReader(uncompressedReader)
}
