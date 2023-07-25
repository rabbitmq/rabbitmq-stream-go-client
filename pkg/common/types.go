package common

import (
	"bytes"
	"compress/gzip"
	"encoding"
	"github.com/rabbitmq/rabbitmq-stream-go-client/v2/pkg/constants"
)

// Serializer is something that can encode and decode itself
// into a binary sequence
type Serializer interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

//******** Sub Batch Entry Serializer ******************************

// Define an interface for compressing the messages
// Default implementations are
// - NoneCompress: No compression
// - GzipCompress: Gzip compression
// - SnappyCompress: Snappy compression not shipped with this library
// - Lz4Compress: Lz4 compression not shipped with this library
// - ZstdCompress: Zstd compression not shipped with this library
// To be compliant with the Java/DotNET client the client should follow the same compression algorithm
// with the same order (0,1,2,3,4).
// see constants.CompressionNone, constants.CompressionGzip

type CompresserCodec interface {
	Compress(subBatchMessages []byte) ([]byte, error)
	GetType() uint8
}

// CompressNONE No compression
type CompressNONE struct {
}

func (es *CompressNONE) Compress(subBatchMessages []byte) ([]byte, error) {
	return subBatchMessages, nil
}

func (es *CompressNONE) GetType() uint8 {
	return constants.CompressionNone
}

// CompressGZIP Gzip compression
type CompressGZIP struct {
}

func (es *CompressGZIP) Compress(subBatchMessages []byte) ([]byte, error) {
	var tmp bytes.Buffer
	w := gzip.NewWriter(&tmp)
	_, err := w.Write(subBatchMessages)
	if err != nil {
		return nil, err
	}

	err = w.Flush()
	if err != nil {
		return nil, err
	}

	err = w.Close()
	if err != nil {
		return nil, err
	}

	return tmp.Bytes(), nil
}

func (es *CompressGZIP) GetType() uint8 {
	return constants.CompressionGzip
}

//****************************************************************
