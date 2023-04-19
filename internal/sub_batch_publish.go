package internal

import (
	"bufio"
)

type SubBatchPublishRequest struct {
	publisherId           uint8
	numberOfRootMessages  int    // number of root messages in the batch, in this case will be always one
	publishingId          uint64 // publishing id for the sub batch. So there is a relation like: 1- publishingId to N - sub batch messages
	compressType          uint8  // See constants.CompressType and the common/types::CompresserCodec interface
	subBatchMessagesCount uint16 // number of sub batch messages
	unCompressedDataSize  int    // uncompressed data size. The messages size without compression
	compressedDataSize    int    // compressed data size. The messages size with compression
	subBatchMessages      []byte // generic byte array with the sub batch messages
}

func (p *SubBatchPublishRequest) Key() uint16 {
	return CommandPublish
}

func (p *SubBatchPublishRequest) Version() int16 {
	return Version1
}

func (p *SubBatchPublishRequest) Write(writer *bufio.Writer) (int, error) {
	many, err := writeMany(writer,
		p.publisherId,
		p.numberOfRootMessages,
		p.publishingId,
		p.compressType,
		p.subBatchMessagesCount,
		p.unCompressedDataSize,
		p.compressedDataSize,
		p.subBatchMessages)
	return many, err
}

func (p *SubBatchPublishRequest) SizeNeeded() int {
	return streamProtocolKeySizeBytes +
		streamProtocolVersionSizeBytes +
		streamProtocolKeySizeUint8 + // publisherId
		streamProtocolKeySizeUint32 + // numberOfRootMessages
		streamProtocolKeySizeUint64 + //publishingId
		streamProtocolKeySizeUint8 + // compressType
		streamProtocolKeySizeUint16 + // subEntryMessagesCount
		streamProtocolKeySizeUint32 + // uncompressedDataSize
		streamProtocolKeySizeUint32 + // compressedDataSize
		p.compressedDataSize
}

func NewSubBatchPublishRequest(publisherId uint8, numberOfRootMessages int, publishingId uint64, compressType uint8, subBatchMessagesCount uint16, unCompressedDataSize int, compressedDataSize int, subBatchMessages []byte) *SubBatchPublishRequest {
	agg := 0x80 | compressType<<4
	return &SubBatchPublishRequest{publisherId: publisherId, numberOfRootMessages: numberOfRootMessages, publishingId: publishingId, compressType: agg, subBatchMessagesCount: subBatchMessagesCount, unCompressedDataSize: unCompressedDataSize, compressedDataSize: compressedDataSize, subBatchMessages: subBatchMessages}
}
