package stream

type Stats struct {
	firstOffset      int64
	committedChunkId int64
}

func (s Stats) FirstOffset() int64 {
	return s.firstOffset
}

func (s Stats) CommittedChunkId() int64 {
	return s.committedChunkId
}
