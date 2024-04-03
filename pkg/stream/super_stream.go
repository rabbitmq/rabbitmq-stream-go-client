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

const maxAge = "max-age"
const maxLengthBytes = "max-length-bytes"
const queueLeaderLocator = "queue-leader-locator"
const streamMaxSegmentSizeBytes = "stream-max-segment-size-bytes"

type SuperStreamOptions interface {
	getPartitions(prefix string) []string
	getBindingKeys() []string
	getArgs() map[string]string
}

type PartitionsSuperStreamOptions struct {
	Partitions          int
	MaxAge              time.Duration
	MaxLengthBytes      *ByteCapacity
	MaxSegmentSizeBytes *ByteCapacity
	LeaderLocator       string
	args                map[string]string
}

func NewPartitionsSuperStreamOptions(partitions int) *PartitionsSuperStreamOptions {
	return &PartitionsSuperStreamOptions{
		Partitions: partitions,
		args:       make(map[string]string),
	}
}

func (t *PartitionsSuperStreamOptions) SetMaxAge(maxAge time.Duration) *PartitionsSuperStreamOptions {
	t.MaxAge = maxAge
	return t
}

func (t *PartitionsSuperStreamOptions) SetMaxLengthBytes(maxLengthBytes *ByteCapacity) *PartitionsSuperStreamOptions {
	t.MaxLengthBytes = maxLengthBytes
	return t
}

func (t *PartitionsSuperStreamOptions) SetMaxSegmentSizeBytes(maxSegmentSizeBytes *ByteCapacity) *PartitionsSuperStreamOptions {
	t.MaxSegmentSizeBytes = maxSegmentSizeBytes
	return t
}

func (t *PartitionsSuperStreamOptions) SetLeaderLocator(leaderLocator string) *PartitionsSuperStreamOptions {
	t.LeaderLocator = leaderLocator
	return t
}

func (t *PartitionsSuperStreamOptions) getPartitions(prefix string) []string {
	var partitions []string
	for i := 0; i < t.Partitions; i++ {
		partitions = append(partitions, fmt.Sprintf("%s-%d", prefix, i))

	}
	return partitions
}

func (t *PartitionsSuperStreamOptions) getBindingKeys() []string {
	var bindingKeys []string
	for i := 0; i < t.Partitions; i++ {
		bindingKeys = append(bindingKeys, fmt.Sprintf("%d", i))
	}
	return bindingKeys
}

func (t *PartitionsSuperStreamOptions) getArgs() map[string]string {
	if t.MaxAge > 0 {
		t.args[maxAge] = fmt.Sprintf("%ds", int(t.MaxAge.Seconds()))
	}
	if t.MaxLengthBytes != nil {
		t.args[maxLengthBytes] = fmt.Sprintf("%d", t.MaxLengthBytes.bytes)
	}
	if t.MaxSegmentSizeBytes != nil {
		t.args[streamMaxSegmentSizeBytes] = fmt.Sprintf("%d", t.MaxSegmentSizeBytes.bytes)
	}
	if t.LeaderLocator != "" {
		t.args[queueLeaderLocator] = t.LeaderLocator
	}
	return t.args
}

type BindingsSuperStreamOptions struct {
	Bindings            []string
	MaxAge              time.Duration
	MaxLengthBytes      *ByteCapacity
	MaxSegmentSizeBytes *ByteCapacity
	LeaderLocator       string
	args                map[string]string
}

func NewBindingsSuperStreamOptions(bindings []string) *BindingsSuperStreamOptions {
	return &BindingsSuperStreamOptions{
		Bindings: bindings,
		args:     make(map[string]string),
	}
}

func (t *BindingsSuperStreamOptions) SetMaxAge(maxAge time.Duration) *BindingsSuperStreamOptions {
	t.MaxAge = maxAge
	return t
}

func (t *BindingsSuperStreamOptions) SetMaxLengthBytes(maxLengthBytes *ByteCapacity) *BindingsSuperStreamOptions {
	t.MaxLengthBytes = maxLengthBytes
	return t
}

func (t *BindingsSuperStreamOptions) SetMaxSegmentSizeBytes(maxSegmentSizeBytes *ByteCapacity) *BindingsSuperStreamOptions {
	t.MaxSegmentSizeBytes = maxSegmentSizeBytes
	return t
}

func (t *BindingsSuperStreamOptions) SetLeaderLocator(leaderLocator string) *BindingsSuperStreamOptions {
	t.LeaderLocator = leaderLocator
	return t
}

func (t *BindingsSuperStreamOptions) getPartitions(prefix string) []string {
	var partitions []string
	for _, bindingKey := range t.Bindings {
		partitions = append(partitions, fmt.Sprintf("%s-%s", prefix, bindingKey))
	}
	return partitions
}

func (t *BindingsSuperStreamOptions) getBindingKeys() []string {
	return t.Bindings
}

func (t *BindingsSuperStreamOptions) getArgs() map[string]string {
	if t.MaxAge > 0 {
		t.args[maxAge] = fmt.Sprintf("%ds", int(t.MaxAge.Seconds()))
	}
	if t.MaxLengthBytes != nil {
		t.args[maxLengthBytes] = fmt.Sprintf("%d", t.MaxLengthBytes.bytes)
	}
	if t.MaxSegmentSizeBytes != nil {
		t.args[streamMaxSegmentSizeBytes] = fmt.Sprintf("%d", t.MaxSegmentSizeBytes.bytes)
	}
	if t.LeaderLocator != "" {
		t.args[queueLeaderLocator] = t.LeaderLocator
	}
	return t.args
}
