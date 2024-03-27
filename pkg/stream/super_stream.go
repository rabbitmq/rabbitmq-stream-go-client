package stream

import (
	"fmt"
	"time"
)

//public TimeSpan MaxAge
//{
//set => Args["max-age"] = $"{value.TotalSeconds}s";
//}
//
//public ulong MaxLengthBytes
//{
//set => Args["max-length-bytes"] = $"{value}";
//}
//
//public LeaderLocator LeaderLocator
//{
//set => Args["queue-leader-locator"] = $"{value.ToString()}";
//}
//
//public int MaxSegmentSizeBytes
//{
//set => Args["stream-max-segment-size-bytes"] = $"{value}";
//}

const max_age = "max-age"
const max_length_bytes = "max-length-bytes"
const queue_leader_locator = "queue-leader-locator"
const stream_max_segment_size_bytes = "stream-max-segment-size-bytes"

type SuperStreamOptions interface {
	getPartitions(prefix string) []string
	getBindingKeys() []string
	getArgs() map[string]string
}

type PartitionSuperStreamOptions struct {
	Partitions int

	MaxAge              time.Duration
	MaxLengthBytes      *ByteCapacity
	MaxSegmentSizeBytes *ByteCapacity

	LeaderLocator string
	args          map[string]string
}

func NewPartitionSuperStreamOptions(partitions int) *PartitionSuperStreamOptions {
	return &PartitionSuperStreamOptions{
		Partitions: partitions,
		args:       make(map[string]string),
	}
}

func (t *PartitionSuperStreamOptions) SetMaxAge(maxAge time.Duration) *PartitionSuperStreamOptions {
	t.MaxAge = maxAge
	return t
}

func (t *PartitionSuperStreamOptions) SetMaxLengthBytes(maxLengthBytes *ByteCapacity) *PartitionSuperStreamOptions {
	t.MaxLengthBytes = maxLengthBytes
	return t
}

func (t *PartitionSuperStreamOptions) SetMaxSegmentSizeBytes(maxSegmentSizeBytes *ByteCapacity) *PartitionSuperStreamOptions {
	t.MaxSegmentSizeBytes = maxSegmentSizeBytes
	return t
}

func (t *PartitionSuperStreamOptions) SetLeaderLocator(leaderLocator string) *PartitionSuperStreamOptions {
	t.LeaderLocator = leaderLocator
	return t
}

func (t *PartitionSuperStreamOptions) getPartitions(prefix string) []string {
	var partitions []string
	for i := 0; i < t.Partitions; i++ {
		partitions = append(partitions, fmt.Sprintf("%s-%d", prefix, i))

	}
	return partitions
}

func (t *PartitionSuperStreamOptions) getBindingKeys() []string {
	var bindingKeys []string
	for i := 0; i < t.Partitions; i++ {
		bindingKeys = append(bindingKeys, fmt.Sprintf("%d", i))
	}
	return bindingKeys
}

func (t *PartitionSuperStreamOptions) getArgs() map[string]string {
	if t.MaxAge > 0 {
		t.args[max_age] = fmt.Sprintf("%ds", int(t.MaxAge.Seconds()))
	}
	if t.MaxLengthBytes != nil {
		t.args[max_length_bytes] = fmt.Sprintf("%d", t.MaxLengthBytes.bytes)
	}
	if t.MaxSegmentSizeBytes != nil {
		t.args[stream_max_segment_size_bytes] = fmt.Sprintf("%d", t.MaxSegmentSizeBytes.bytes)
	}
	if t.LeaderLocator != "" {
		t.args[queue_leader_locator] = t.LeaderLocator
	}
	return t.args
}
