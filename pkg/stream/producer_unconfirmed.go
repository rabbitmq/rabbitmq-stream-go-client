package stream

import (
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
	messages map[int64]*ConfirmationStatus
	mutex    sync.RWMutex
}

const DefaultUnconfirmedSize = 10000

func newUnConfirmed() *unConfirmed {

	r := &unConfirmed{
		messages: make(map[int64]*ConfirmationStatus, DefaultUnconfirmedSize),
		mutex:    sync.RWMutex{},
	}

	return r
}

func (u *unConfirmed) addBatch(messageSequences []*messageSequence, producerID uint8) {

	u.mutex.Lock()
	for _, ms := range messageSequences {
		u.messages[ms.publishingId] = &ConfirmationStatus{
			inserted:     time.Now(),
			message:      *ms.refMessage,
			producerID:   producerID,
			publishingId: ms.publishingId,
			confirmed:    false,
		}
	}
	u.mutex.Unlock()

}

func (u *unConfirmed) add(id int64, cf *ConfirmationStatus) {
	u.mutex.Lock()
	u.messages[id] = cf
	u.mutex.Unlock()
}

func (u *unConfirmed) removeBatch(confirmationStatus []*ConfirmationStatus) {
	u.mutex.Lock()
	for _, cs := range confirmationStatus {
		delete(u.messages, cs.publishingId)
	}
	u.mutex.Unlock()

}

func (u *unConfirmed) remove(id int64) {
	u.mutex.Lock()
	delete(u.messages, id)
	u.mutex.Unlock()
}

func (u *unConfirmed) get(id int64) *ConfirmationStatus {
	u.mutex.RLock()
	defer u.mutex.RUnlock()
	return u.messages[id]
}

func (u *unConfirmed) size() int {
	u.mutex.RLock()
	defer u.mutex.RUnlock()
	return len(u.messages)
}

func (u *unConfirmed) getAll() map[int64]*ConfirmationStatus {
	cloned := make(map[int64]*ConfirmationStatus, len(u.messages))
	u.mutex.RLock()
	defer u.mutex.RUnlock()
	for k, v := range u.messages {
		cloned[k] = v
	}
	return cloned

}

func (u *unConfirmed) clear() {
	u.mutex.Lock()
	u.messages = make(map[int64]*ConfirmationStatus, DefaultUnconfirmedSize)
	u.mutex.Unlock()
}
