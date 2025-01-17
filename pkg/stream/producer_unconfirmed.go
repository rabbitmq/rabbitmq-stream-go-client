package stream

import (
	"container/heap"
	"sync"
	"time"
)

type priorityMessage struct {
	*ConfirmationStatus
	index int
}

// priorityQueue implements heap.Interface
type priorityQueue []*priorityMessage

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	// Earlier timestamps have higher priority
	return pq[i].inserted.Before(pq[j].inserted)
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*priorityMessage)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

type unConfirmed struct {
	messages        map[int64]*priorityMessage
	timeoutQueue    priorityQueue
	mutexMessageMap sync.RWMutex
}

const DefaultUnconfirmedSize = 10_000

func newUnConfirmed() *unConfirmed {
	r := &unConfirmed{
		messages:        make(map[int64]*priorityMessage, DefaultUnconfirmedSize),
		timeoutQueue:    make(priorityQueue, 0, DefaultUnconfirmedSize),
		mutexMessageMap: sync.RWMutex{},
	}
	heap.Init(&r.timeoutQueue)
	return r
}

func (u *unConfirmed) addFromSequences(messages []*messageSequence, producerID uint8) {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()

	for _, msgSeq := range messages {
		pm := &priorityMessage{
			ConfirmationStatus: &ConfirmationStatus{
				inserted:     time.Now(),
				message:      msgSeq.sourceMsg,
				producerID:   producerID,
				publishingId: msgSeq.publishingId,
				confirmed:    false,
			},
		}
		u.messages[msgSeq.publishingId] = pm
		heap.Push(&u.timeoutQueue, pm)
	}
}

func (u *unConfirmed) link(from int64, to int64) {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()

	fromMsg := u.messages[from]
	if fromMsg != nil {
		toMsg := u.messages[to]
		if toMsg != nil {
			fromMsg.linkedTo = append(fromMsg.linkedTo, toMsg.ConfirmationStatus)
		}
	}
}

func (u *unConfirmed) extractWithConfirms(ids []int64) []*ConfirmationStatus {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()

	var res []*ConfirmationStatus
	for _, v := range ids {
		if msg := u.extract(v, 0, true); msg != nil {
			res = append(res, msg)
			if msg.linkedTo != nil {
				res = append(res, msg.linkedTo...)
			}
		}
	}
	return res
}

func (u *unConfirmed) extractWithError(id int64, errorCode uint16) *ConfirmationStatus {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()
	return u.extract(id, errorCode, false)
}

func (u *unConfirmed) extract(id int64, errorCode uint16, confirmed bool) *ConfirmationStatus {
	pm := u.messages[id]
	if pm == nil {
		return nil
	}

	rootMessage := pm.ConfirmationStatus
	u.updateStatus(rootMessage, errorCode, confirmed)

	for _, linkedMessage := range rootMessage.linkedTo {
		u.updateStatus(linkedMessage, errorCode, confirmed)
		if linkedPm := u.messages[linkedMessage.publishingId]; linkedPm != nil {
			// Remove from priority queue if exists
			if linkedPm.index != -1 {
				heap.Remove(&u.timeoutQueue, linkedPm.index)
			}
			delete(u.messages, linkedMessage.publishingId)
		}
	}

	// Remove from priority queue if exists
	if pm.index != -1 {
		heap.Remove(&u.timeoutQueue, pm.index)
	}
	delete(u.messages, id)

	return rootMessage
}

func (u *unConfirmed) updateStatus(rootMessage *ConfirmationStatus, errorCode uint16, confirmed bool) {
	rootMessage.confirmed = confirmed
	if confirmed {
		return
	}
	rootMessage.errorCode = errorCode
	rootMessage.err = lookErrorCode(errorCode)
}

func (u *unConfirmed) extractWithTimeOut(timeout time.Duration) []*ConfirmationStatus {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()

	var res []*ConfirmationStatus
	now := time.Now()

	for u.timeoutQueue.Len() > 0 {
		pm := u.timeoutQueue[0]
		if now.Sub(pm.inserted) < timeout {
			break
		}

		heap.Pop(&u.timeoutQueue)
		if msg := u.extract(pm.publishingId, timeoutError, false); msg != nil {
			res = append(res, msg)
		}
	}

	return res
}

func (u *unConfirmed) size() int {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()
	return len(u.messages)
}
