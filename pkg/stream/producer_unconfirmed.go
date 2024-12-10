package stream

import "sync"

type unConfirmedPartition struct {
	messages map[int64]*ConfirmationStatus
	mutex    sync.Mutex
}

func newUnConfirmedPartition() *unConfirmedPartition {
	return &unConfirmedPartition{
		messages: make(map[int64]*ConfirmationStatus),
	}
}

func (u *unConfirmedPartition) add(id int64, cf *ConfirmationStatus) {
	u.mutex.Lock()
	defer u.mutex.Unlock()
	u.messages[id] = cf
}

func (u *unConfirmedPartition) remove(id int64) {
	u.mutex.Lock()
	defer u.mutex.Unlock()
	delete(u.messages, id)
}

func (u *unConfirmedPartition) get(id int64) *ConfirmationStatus {
	u.mutex.Lock()
	defer u.mutex.Unlock()
	return u.messages[id]
}

func (u *unConfirmedPartition) size() int {
	u.mutex.Lock()
	defer u.mutex.Unlock()
	return len(u.messages)
}
func (u *unConfirmedPartition) clear() {
	u.mutex.Lock()
	defer u.mutex.Unlock()
	u.messages = make(map[int64]*ConfirmationStatus)
}

func (u *unConfirmedPartition) getAll() map[int64]*ConfirmationStatus {
	u.mutex.Lock()
	defer u.mutex.Unlock()
	result := make(map[int64]*ConfirmationStatus)
	for i, message := range u.messages {
		result[i] = message
	}
	return result
}

type unConfirmed struct {
	partitions      []*unConfirmedPartition
	partitionNumber int
}

func newUnConfirmed(partitionNumber int) *unConfirmed {
	var partitions []*unConfirmedPartition
	for i := 0; i < partitionNumber; i++ {
		partitions = append(partitions, newUnConfirmedPartition())
	}
	return &unConfirmed{
		partitions:      partitions,
		partitionNumber: partitionNumber,
	}
}

func (u *unConfirmed) add(id int64, cf *ConfirmationStatus) {
	partition := id % int64(u.partitionNumber)
	u.partitions[partition].add(id, cf)
}

func (u *unConfirmed) remove(id int64) {
	partition := id % int64(u.partitionNumber)
	u.partitions[partition].remove(id)
}

func (u *unConfirmed) get(id int64) *ConfirmationStatus {
	partition := id % int64(u.partitionNumber)
	return u.partitions[partition].get(id)
}

func (u *unConfirmed) size() int {
	size := 0
	for _, partition := range u.partitions {
		size += partition.size()
	}
	return size
}

func (u *unConfirmed) getAll() map[int64]*ConfirmationStatus {
	result := make(map[int64]*ConfirmationStatus)
	for _, partition := range u.partitions {
		for _, status := range partition.getAll() {
			result[status.publishingId] = status
		}
	}
	return result
}

func (u *unConfirmed) clear() {
	for _, partition := range u.partitions {
		partition.clear()
	}
}
