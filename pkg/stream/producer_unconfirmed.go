package stream

import (
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/logs"
	"sync"
	"time"
)

// unConfirmed is a structure that holds unconfirmed messages
// And unconfirmed message is a message that has been sent to the broker but not yet confirmed,
// and it is added to the unConfirmed structure as soon is possible when
//
//	the Send() or BatchSend() method is called
//
// The confirmation status is updated when the confirmation is received from the broker (see server_frame.go)
// or due of timeout. The Timeout is configurable, and it is calculated client side.
type unConfirmed struct {
	messages        map[int64]*ConfirmationStatus
	mutexMessageMap sync.RWMutex
	maxSize         int
	blockSignal     *sync.Cond
}

func newUnConfirmed(maxSize int) *unConfirmed {
	r := &unConfirmed{
		messages:        make(map[int64]*ConfirmationStatus, maxSize),
		mutexMessageMap: sync.RWMutex{},
		maxSize:         maxSize,
		blockSignal:     sync.NewCond(&sync.Mutex{}),
	}
	return r
}

func (u *unConfirmed) addFromSequences(messages []*messageSequence, producerID uint8) {

	if u.size() > u.maxSize {
		logs.LogDebug("unConfirmed size: %d reached, producer blocked", u.maxSize)
		u.blockSignal.L.Lock()
		u.blockSignal.Wait()
		u.blockSignal.L.Unlock()
	}

	u.mutexMessageMap.Lock()
	for _, msgSeq := range messages {
		u.messages[msgSeq.publishingId] = &ConfirmationStatus{
			inserted:     time.Now(),
			message:      msgSeq.sourceMsg,
			producerID:   producerID,
			publishingId: msgSeq.publishingId,
			confirmed:    false,
		}
	}
	u.mutexMessageMap.Unlock()

}

func (u *unConfirmed) link(from int64, to int64) {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()
	r := u.messages[from]
	if r != nil {
		r.linkedTo = append(r.linkedTo, u.messages[to])
	}
}

func (u *unConfirmed) extractWithConfirms(ids []int64) []*ConfirmationStatus {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()

	res := make([]*ConfirmationStatus, 0, len(ids))
	for _, v := range ids {
		m := u.extract(v, 0, true)
		if m != nil {
			res = append(res, m)
			if m.linkedTo != nil {
				res = append(res, m.linkedTo...)
			}
		}
	}
	u.maybeUnLock()
	return res
}

func (u *unConfirmed) extractWithError(id int64, errorCode uint16) *ConfirmationStatus {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()
	cs := u.extract(id, errorCode, false)
	u.maybeUnLock()
	return cs
	return u.extract(id, errorCode, false)
}

func (u *unConfirmed) extract(id int64, errorCode uint16, confirmed bool) *ConfirmationStatus {
	rootMessage := u.messages[id]
	if rootMessage != nil {
		u.updateStatus(rootMessage, errorCode, confirmed)

		for _, linkedMessage := range rootMessage.linkedTo {
			u.updateStatus(linkedMessage, errorCode, confirmed)
			delete(u.messages, linkedMessage.publishingId)
		}
		delete(u.messages, id)
	}
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
	for _, v := range u.messages {
		if time.Since(v.inserted) > timeout {
			v := u.extract(v.publishingId, timeoutError, false)
			res = append(res, v)
		}
	}
	u.maybeUnLock()
	return res
}

func (u *unConfirmed) size() int {
	u.mutexMessageMap.Lock()
	defer u.mutexMessageMap.Unlock()
	return len(u.messages)
}

func (u *unConfirmed) maybeUnLock() {
	if len(u.messages) < u.maxSize {
		logs.LogDebug("unConfirmed size: %d back to normal, producer unblocked", u.maxSize)
		u.blockSignal.L.Lock()
		u.blockSignal.Broadcast()
		u.blockSignal.L.Unlock()
	}
}
