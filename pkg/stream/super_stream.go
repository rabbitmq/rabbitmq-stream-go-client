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

type PartitionsOptions struct {
	Partitions          int
	MaxAge              time.Duration
	MaxLengthBytes      *ByteCapacity
	MaxSegmentSizeBytes *ByteCapacity
	LeaderLocator       string
	args                map[string]string
}

func NewPartitionsOptions(partitions int) *PartitionsOptions {
	return &PartitionsOptions{
		Partitions: partitions,
		args:       make(map[string]string),
	}
}

func (t *PartitionsOptions) SetMaxAge(maxAge time.Duration) *PartitionsOptions {
	t.MaxAge = maxAge
	return t
}

func (t *PartitionsOptions) SetMaxLengthBytes(maxLengthBytes *ByteCapacity) *PartitionsOptions {
	t.MaxLengthBytes = maxLengthBytes
	return t
}

func (t *PartitionsOptions) SetMaxSegmentSizeBytes(maxSegmentSizeBytes *ByteCapacity) *PartitionsOptions {
	t.MaxSegmentSizeBytes = maxSegmentSizeBytes
	return t
}

func (t *PartitionsOptions) SetBalancedLeaderLocator() *PartitionsOptions {
	t.LeaderLocator = LeaderLocatorBalanced
	return t
}

func (t *PartitionsOptions) SetClientLocalLocator() *PartitionsOptions {
	t.LeaderLocator = LeaderLocatorClientLocal
	return t
}

func (t *PartitionsOptions) getPartitions(prefix string) []string {
	var partitions []string
	for i := 0; i < t.Partitions; i++ {
		partitions = append(partitions, fmt.Sprintf("%s-%d", prefix, i))

	}
	return partitions
}

func (t *PartitionsOptions) getBindingKeys() []string {
	var bindingKeys []string
	for i := 0; i < t.Partitions; i++ {
		bindingKeys = append(bindingKeys, fmt.Sprintf("%d", i))
	}
	return bindingKeys
}

func (t *PartitionsOptions) getArgs() map[string]string {
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

type BindingsOptions struct {
	Bindings            []string
	MaxAge              time.Duration
	MaxLengthBytes      *ByteCapacity
	MaxSegmentSizeBytes *ByteCapacity
	LeaderLocator       string
	args                map[string]string
}

func NewBindingsOptions(bindings []string) *BindingsOptions {
	return &BindingsOptions{
		Bindings: bindings,
		args:     make(map[string]string),
	}
}

func (t *BindingsOptions) SetMaxAge(maxAge time.Duration) *BindingsOptions {
	t.MaxAge = maxAge
	return t
}

func (t *BindingsOptions) SetMaxLengthBytes(maxLengthBytes *ByteCapacity) *BindingsOptions {
	t.MaxLengthBytes = maxLengthBytes
	return t
}

func (t *BindingsOptions) SetMaxSegmentSizeBytes(maxSegmentSizeBytes *ByteCapacity) *BindingsOptions {
	t.MaxSegmentSizeBytes = maxSegmentSizeBytes
	return t
}

func (t *BindingsOptions) SetBalancedLeaderLocator() *BindingsOptions {
	t.LeaderLocator = LeaderLocatorBalanced
	return t
}

func (t *BindingsOptions) SetClientLocalLocator() *BindingsOptions {
	t.LeaderLocator = LeaderLocatorClientLocal
	return t
}

func (t *BindingsOptions) getPartitions(prefix string) []string {
	var partitions []string
	for _, bindingKey := range t.Bindings {
		partitions = append(partitions, fmt.Sprintf("%s-%s", prefix, bindingKey))
	}
	return partitions
}

func (t *BindingsOptions) getBindingKeys() []string {
	return t.Bindings
}

func (t *BindingsOptions) getArgs() map[string]string {
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
