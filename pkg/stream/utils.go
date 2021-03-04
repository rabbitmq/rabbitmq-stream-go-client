package stream

func UShortExtractResponseCode(code uint16) uint16 {
	return code & 0b0111_1111_1111_1111
}

func UIntExtractResponseCode(code int32) int32 {
	return code & 0b0111_1111_1111_1111
}

func UShortEncodeResponseCode(code uint16) uint16 {
	return code | 0b1000_0000_0000_0000
}
