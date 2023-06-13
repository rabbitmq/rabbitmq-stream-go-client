package constants

const (
	OffsetTypeFirst     uint16 = 0x01
	OffsetTypeLast      uint16 = 0x02
	OffsetTypeNext      uint16 = 0x03
	OffsetTypeOffset    uint16 = 0x04
	OffsetTypeTimeStamp uint16 = 0x05
)

// CompressionType is the type of compression used for subBatchEntry
// See common/types.go Compresser interface
// See client::SendSubEntryBatch
const (
	CompressionNone uint8 = 0x00
	CompressionGzip uint8 = 0x01
)
