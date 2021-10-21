package stream

import (
	"bytes"
	"compress/gzip"
)

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
}

func compressByType(compression Compression) iCompress {

	switch compression.value {
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
