package internal

func sizeNeededForMap(m map[string]string) (n int) {
	n = streamProtocolMapLenBytes
	for k, v := range m {
		n += streamProtocolStringLenSizeBytes + len(k)
		n += streamProtocolStringLenSizeBytes + len(v)
	}
	return
}
