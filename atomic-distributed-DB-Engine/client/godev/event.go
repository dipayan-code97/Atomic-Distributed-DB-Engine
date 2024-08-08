package godev

import (
	"github.com/snower/slock/protocol"
	"sync"
	"time"
)

const EVENT_MODE_DEFAULT_SET = 0
const EVENT_MODE_DEFAULT_CLEAR = 1

type Event struct {
	db        *Database
	eventKey  [16]byte
	timeout   uint32
	expried   uint32
	eventLock *Lock
	checkLock *Lock
	waitLock  *Lock
	glock     *sync.Mutex
	setedMode uint8
}

func NewEvent(db *Database, eventKey [16]byte, timeout uint32, expried uint32) *Event {
	return &Event{db, eventKey, timeout, expried, nil,
		nil, nil, &sync.Mutex{}, EVENT_MODE_DEFAULT_SET}
}

func NewDefaultSetEvent(db *Database, eventKey [16]byte, timeout time.Duration, expried uint32) *Event {
	return &Event{db, eventKey, timeout, expried, nil,
		nil, nil, &sync.Mutex{}, EVENT_MODE_DEFAULT_SET}
}

func NewDefaultClearEvent(db *Database, eventKey [16]byte, timeout time.Duration, expried uint32) *Event {
	return &Event{db, eventKey, timeout, expried, nil,
		nil, nil, &sync.Mutex{}, EVENT_MODE_DEFAULT_CLEAR}
}

func (self *Event) GetEventKey() [16]byte {
	return self.eventKey
}

func (self *Event) GetTimeout() uint32 {
	return self.timeout
}

func (self *Event) GetExpried() uint32 {
	return self.expried
}

func (self *Event) Mode() uint8 {
	return self.setedMode
}

func (self *Event) Clear() error {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.glock.Lock()
		if self.eventLock == nil {
			self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 0, 0}
		}
		self.glock.Unlock()

		err := self.eventLock.LockUpdate()
		if err.Result == protocol.RESULT_SUCCEED {
			return nil
		}
		return err
	}

	self.glock.Lock()
	if self.eventLock == nil {
		self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 1, 0}
	}
	self.glock.Unlock()

	err := self.eventLock.unlock()
	if err == nil {
		return nil
	}
	if err.Result == protocol.RESULT_UNLOCK_ERROR {
		return nil
	}
	return err
}

func (self *Event) Set() error {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.glock.Lock()
		if self.eventLock == nil {
			self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 0, 0}
		}
		self.glock.Unlock()

		err := self.eventLock.unlock()
		if err == nil {
			return nil
		}
		if err.Result == protocol.RESULT_UNLOCK_ERROR {
			return nil
		}
		return err
	}

	self.glock.Lock()
	if self.eventLock == nil {
		self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 1, 0}
	}
	self.glock.Unlock()

	err := self.eventLock.LockUpdate()
	if err.Result == protocol.RESULT_SUCCEED {
		return nil
	}
	return err
}

func (self *Event) IsSet() (bool, error) {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.checkLock = &Lock{self.db, self.db.generateLockID(), self.eventKey, 0, 0, 0, 0}
		err := self.checkLock.lock()
		if err == nil {
			return true, nil
		}
		if err.Result == protocol.RESULT_TIMEOUT {
			return false, nil
		}
		return false, err
	}

	self.checkLock = &Lock{self.db, self.db.generateLockID(), self.eventKey, 0x02000000, 0, 1, 0}
	err := self.checkLock.lock()
	if err == nil {
		return true, nil
	}
	if err.Result == protocol.RESULT_UNOWN_ERROR || err.Result == protocol.RESULT_TIMEOUT {
		return false, nil
	}
	return false, err
}

func (self *Event) Wait(timeout uint32) (bool, error) {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.waitLock = &Lock{self.db, self.db.generateLockID(), self.eventKey, timeout, 0, 0, 0}
		err := self.waitLock.lock()
		if err == nil {
			return true, nil
		}
		if err.Result == protocol.RESULT_TIMEOUT {
			return false, nil
		}
		return false, err
	}

	self.waitLock = &Lock{self.db, self.db.generateLockID(), self.eventKey, timeout | 0x02000000, 0, 1, 0}
	err := self.waitLock.lock()
	if err == nil {
		return true, nil
	}
	if err.Result == protocol.RESULT_TIMEOUT {
		return false, nil
	}
	return false, err
}

func (self *Event) WaitAndTimeoutRetryClear(timeout uint32) (bool, error) {
	if self.setedMode == EVENT_MODE_DEFAULT_SET {
		self.waitLock = &Lock{self.db, self.db.generateLockID(), self.eventKey, timeout, 0, 0, 0}
		err := self.waitLock.lock()
		if err == nil {
			return true, nil
		}

		if err.Result == protocol.RESULT_TIMEOUT {
			self.glock.Lock()
			if self.eventLock == nil {
				self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 0, 0}
			}
			self.glock.Unlock()

			rerr := self.eventLock.LockUpdate()
			if rerr.Result == 0 {
				if rerr.CommandResult.Result == protocol.RESULT_SUCCEED {
					_ = self.eventLock.unlock()
					return true, nil
				}
				if rerr.CommandResult.Result == protocol.RESULT_LOCKED_ERROR {
					return false, nil
				}
			}
		}
		return false, err
	}

	self.waitLock = &Lock{self.db, self.db.generateLockID(), self.eventKey, timeout | 0x02000000, 0, 1, 0}
	err := self.waitLock.lock()
	if err == nil {
		self.glock.Lock()
		if self.eventLock == nil {
			self.eventLock = &Lock{self.db, self.eventKey, self.eventKey, self.timeout, self.expried, 1, 0}
		}
		self.glock.Unlock()
		_ = self.eventLock.unlock()
		return true, nil
	}
	if err.Result == protocol.RESULT_TIMEOUT {
		return false, nil
	}
	return false, err
}
