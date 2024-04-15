package stream

import (
	"bufio"
	"bytes"
	"sync"
	"time"
)

// Coordinator

type deliverQueue struct {
	chSignal chan struct{}

	currentDelivers       []int64
	expectedToBeDelivered int32
	mutex                 *sync.Mutex
}

func newDeliverQueue() *deliverQueue {
	return &deliverQueue{
		chSignal: make(chan struct{}, 1),
		mutex:    &sync.Mutex{}}
}

func (dq *deliverQueue) getFirst() int64 {
	dq.mutex.Lock()
	defer dq.mutex.Unlock()
	if len(dq.currentDelivers) == 0 {
		return -1
	}
	return dq.currentDelivers[0]
}

func (dq *deliverQueue) push(id int64, data []byte,
	filter bool, offset int64, offsetLimit int64,
	numRecords int,
	consumer *Consumer) {

	dq.chSignal <- struct{}{}
	dq.mutex.Lock()
	dq.currentDelivers = append(dq.currentDelivers, id)
	dq.mutex.Unlock()

	go func(id int64, data []byte, filter bool, offset int64, offsetLimit int64, numRecords int, consumer *Consumer) {
		var batchConsumingMessages offsetMessages
		bufferReader := bytes.NewReader(data)
		dataReader := bufio.NewReader(bufferReader)
		entryType, err := peekByte(dataReader)
		logErrorCommand(err, "error peeking byte")
		for numRecords > 0 {
			if (entryType & 0x80) == 0 {
				batchConsumingMessages = decodeMessage(dataReader,
					filter,
					offset,
					offsetLimit,
					batchConsumingMessages)
				numRecords--
				offset++
			}
		}

		for dq.getFirst() != id {
			time.Sleep(1 * time.Microsecond)
		}

		//fmt.Printf("OK %v id: %d\n", dq.currentDelivers, id)

		consumer.response.chunkForConsumer <- chunkInfo{offsetMessages: batchConsumingMessages, numEntries: uint16(len(batchConsumingMessages))}

		dq.mutex.Lock()
		dq.currentDelivers = remove(dq.currentDelivers, id)
		dq.mutex.Unlock()
		<-dq.chSignal
	}(id, data, filter, offset, offsetLimit, numRecords, consumer)
}

func remove(slice []int64, value int64) []int64 {
	for i, v := range slice {
		if v == value {
			return append(slice[:i], slice[i+1:]...)
		}
	}
	return slice
}
