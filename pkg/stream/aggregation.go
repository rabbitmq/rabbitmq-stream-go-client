package stream

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
)

const (
	None   = byte(0)
	GZIP   = byte(1)
	SNAPPY = byte(2)
	LZ4    = byte(3)
	ZSTD   = byte(4)
)

type Compression struct {
	enabled bool
	value   byte
}

func (compression Compression) String() string {
	return fmt.Sprintf("value %d (enable: %t)", compression.value,
		compression.enabled)
}

func (compression Compression) None() Compression {
	return Compression{value: None, enabled: true}
}

func (compression Compression) Gzip() Compression {
	return Compression{value: GZIP, enabled: true}
}

func (compression Compression) Snappy() Compression {
	return Compression{value: SNAPPY, enabled: true}
}

func (compression Compression) Zstd() Compression {
	return Compression{value: ZSTD, enabled: true}
}

func (compression Compression) Lz4() Compression {
	return Compression{value: LZ4, enabled: true}
}

type subEntry struct {
	messages     []*messageSequence
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
	Compress(subEntries *subEntries) error
	UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) (*bufio.Reader, error)
}

func compressByValue(value byte) iCompress {
	switch value {
	case GZIP:
		return compressGZIP{}
	case SNAPPY:
		return compressSnappy{}
	case LZ4:
		return compressLZ4{}
	case ZSTD:
		return compressZSTD{}
	}

	return compressNONE{}
}

type compressNONE struct{}

func (es compressNONE) Compress(subEntries *subEntries) error {
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

	return nil
}

func (es compressNONE) UnCompress(source *bufio.Reader, _, _ uint32) (*bufio.Reader, error) {
	return source, nil
}

type compressGZIP struct {
}

func (es compressGZIP) Compress(subEntries *subEntries) error {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		w := gzip.NewWriter(&tmp)

		for _, msg := range entry.messages {
			prefixedMsg := bytesLenghPrefixed(msg.messageBytes)
			if _, err := w.Write(prefixedMsg); err != nil {
				return fmt.Errorf("failed to write message size to gzip writer: %w", err)
			}
		}

		if err := w.Flush(); err != nil {
			return fmt.Errorf("failed to flush gzip writer: %w", err)
		}

		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to close gzip writer: %w", err)
		}

		entry.sizeInBytes += len(tmp.Bytes())
		entry.dataInBytes = tmp.Bytes()
		subEntries.totalSizeInBytes += len(tmp.Bytes())
	}

	return nil
}

func (es compressGZIP) UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) (*bufio.Reader, error) {
	var zipperBuffer = make([]byte, dataSize)
	if _, err := io.ReadFull(source, zipperBuffer); err != nil {
		return nil, fmt.Errorf("GZIP error during reading buffer %s", err)
	}

	reader, err := gzip.NewReader(bytes.NewBuffer(zipperBuffer))
	if err != nil {
		return nil, fmt.Errorf("error creating GZIP NewReader  %s", err)
	}
	//nolint:errcheck
	defer reader.Close()

	// headers --> payload --> headers --> payload (compressed)
	// Read in data.
	uncompressedReader, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error during reading buffer %s", err)
	}

	if uint32(len(uncompressedReader)) != uncompressedDataSize {
		return nil, fmt.Errorf("uncompressedDataSize != count")
	}

	// headers --> payload --> headers --> payload (compressed) --> uncompressed payload
	return bufio.NewReader(bytes.NewReader(uncompressedReader)), nil
}

type compressSnappy struct{}

func (es compressSnappy) Compress(subEntries *subEntries) error {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		w := snappy.NewBufferedWriter(&tmp)
		for _, msg := range entry.messages {
			prefixedMsg := bytesLenghPrefixed(msg.messageBytes)
			if _, err := w.Write(prefixedMsg); err != nil {
				return fmt.Errorf("failed to write message size to snappy writer: %w", err)
			}
		}

		if err := w.Flush(); err != nil {
			return fmt.Errorf("failed to flush snappy writer: %w", err)
		}

		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to close snappy writer: %w", err)
		}

		entry.sizeInBytes += len(tmp.Bytes())
		entry.dataInBytes = tmp.Bytes()
		subEntries.totalSizeInBytes += len(tmp.Bytes())
	}

	return nil
}

func (es compressSnappy) UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) (*bufio.Reader, error) {
	var zipperBuffer = make([]byte, dataSize)
	// array of compress data
	if _, err := io.ReadFull(source, zipperBuffer); err != nil {
		return nil, fmt.Errorf("SNAPPY error during reading buffer %s", err)
	}

	reader := snappy.NewReader(bytes.NewBuffer(zipperBuffer))
	defer reader.Reset(nil)
	// headers --> payload --> headers --> payload (compressed)

	// Read in data.
	uncompressedReader, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error during reading buffer %s", err)
	}

	if uint32(len(uncompressedReader)) != uncompressedDataSize {
		return nil, fmt.Errorf("uncompressedDataSize != count")
	}

	// headers --> payload --> headers --> payload (compressed) --> uncompressed payload
	return bufio.NewReader(bytes.NewReader(uncompressedReader)), nil
}

type compressLZ4 struct{}

func (es compressLZ4) Compress(subEntries *subEntries) error {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		w := lz4.NewWriter(&tmp)

		for _, msg := range entry.messages {
			prefixedMsg := bytesLenghPrefixed(msg.messageBytes)
			if _, err := w.Write(prefixedMsg); err != nil {
				return fmt.Errorf("failed to write message size to LZ4 writer: %w", err)
			}
		}

		if err := w.Flush(); err != nil {
			return fmt.Errorf("failed to flush LZ4 writer: %w", err)
		}

		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to close LZ4 writer: %w", err)
		}

		entry.sizeInBytes += len(tmp.Bytes())
		entry.dataInBytes = tmp.Bytes()
		subEntries.totalSizeInBytes += len(tmp.Bytes())
	}

	return nil
}

func (es compressLZ4) UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) (*bufio.Reader, error) {
	var zipperBuffer = make([]byte, dataSize)
	// array of compress data
	if _, err := io.ReadFull(source, zipperBuffer); err != nil {
		return nil, fmt.Errorf("LZ4 error during reading buffer %s", err)
	}

	reader := lz4.NewReader(bytes.NewBuffer(zipperBuffer))
	defer reader.Reset(nil)
	// headers --> payload --> headers --> payload (compressed)

	// Read in data.
	uncompressedReader, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error during reading buffer %s", err)
	}

	if uint32(len(uncompressedReader)) != uncompressedDataSize {
		return nil, fmt.Errorf("uncompressedDataSize != count")
	}

	// headers --> payload --> headers --> payload (compressed) --> uncompressed payload
	return bufio.NewReader(bytes.NewReader(uncompressedReader)), nil

}

type compressZSTD struct{}

func (es compressZSTD) Compress(subEntries *subEntries) error {
	for _, entry := range subEntries.items {
		var tmp bytes.Buffer
		w, err := zstd.NewWriter(&tmp)
		if err != nil {
			return fmt.Errorf("error creating ZSTD compression algorithm writer %w", err)
		}

		for _, msg := range entry.messages {
			prefixedMsg := bytesLenghPrefixed(msg.messageBytes)
			if _, err := w.Write(prefixedMsg); err != nil {
				return fmt.Errorf("failed to write message size to ZSTD writer: %w", err)
			}
		}

		if err := w.Flush(); err != nil {
			return fmt.Errorf("failed to flush ZSTD writer: %w", err)
		}

		if err := w.Close(); err != nil {
			return fmt.Errorf("failed to close ZSTD writer: %w", err)
		}

		entry.sizeInBytes += len(tmp.Bytes())
		entry.dataInBytes = tmp.Bytes()
		subEntries.totalSizeInBytes += len(tmp.Bytes())
	}

	return nil
}

func (es compressZSTD) UnCompress(source *bufio.Reader, dataSize, uncompressedDataSize uint32) (*bufio.Reader, error) {
	var zipperBuffer = make([]byte, dataSize)
	// array of compress data
	if _, err := io.ReadFull(source, zipperBuffer); err != nil {
		return nil, fmt.Errorf("ZSTD error during reading buffer %s", err)
	}

	reader, err := zstd.NewReader(bytes.NewBuffer(zipperBuffer))
	if err != nil {
		return nil, fmt.Errorf("error creating ZSTD NewReader  %s", err)
	}
	defer reader.Close()
	// headers --> payload --> headers --> payload (compressed)

	// Read in data.
	uncompressedReader, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("error during reading buffer %s", err)
	}

	if uint32(len(uncompressedReader)) != uncompressedDataSize {
		return nil, fmt.Errorf("uncompressedDataSize != count")
	}

	// headers --> payload --> headers --> payload (compressed) --> uncompressed payload
	return bufio.NewReader(bytes.NewReader(uncompressedReader)), nil
}
