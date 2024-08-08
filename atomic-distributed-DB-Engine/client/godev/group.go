package godev

import (
	"github.com/snower/slock/protocol"
	"sync"
)

type GroupEvent struct {
	db          *Database   `json:"db,omitempty"`
	groupKey    [16]byte    `json:"groupKey,omitempty"`
	clientId    uint64      `json:"clientId,omitempty"`
	versionId   uint64      `json:"versionId,omitempty"`
	timeout     uint32      `json:"timeout,omitempty"`
	expired     uint32      `json:"expired,omitempty"`
	eventLocker *Lock       `json:"eventLocker,omitempty"`
	checkLocker *Lock       `json:"checkLocker,omitempty"`
	waitLocker  *Lock       `json:"waitLocker,omitempty"`
	glock       *sync.Mutex `json:"glocker,omitempty"`
}

func BuildNewGroupEvent(database *Database, groupKey [16]byte, clientId, versionId uint64,
	timeout uint32, expired uint32) *GroupEvent {
	return &GroupEvent{
		db:        database,
		groupKey:  groupKey,
		clientId:  clientId,
		versionId: versionId,
		timeout:   timeout,
		expired:   expired,
		glock:     &sync.Mutex{},
	}
}

func (eventRef *GroupEvent) clear() error {
	eventRef.glock.Lock()
	defer eventRef.glock.Unlock()

	// Generate lockId
	lockId := GenerateLockId(eventRef.versionId)

	// Create and update eventLocker
	eventRef.eventLocker = BuildNewLock(eventRef.db, lockId, eventRef.groupKey,
		eventRef.timeout|uint32(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCEED)<<16,
		eventRef.expired, 0, 0)

	// locker update
	err := eventRef.eventLocker.LockUpdate()
	if err.Result != protocol.RESULT_SUCCEED {
		return err
	}
	return nil
}

func GenerateLockId(versionId uint64) [16]byte {
	return [16]byte{
		byte(versionId), byte(versionId >> 8), byte(versionId >> 16), byte(versionId >> 24),
		byte(versionId >> 32), byte(versionId >> 40), byte(versionId >> 48), byte(versionId >> 56),
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	}
}

func (eventRef *GroupEvent) set() error {
	eventRef.glock.Lock()
	defer eventRef.glock.Unlock()

	// Create eventLocker if not already exists
	if eventRef.eventLocker == nil {
		eventRef.eventLocker = BuildNewLock(eventRef.db, [16]byte{}, eventRef.groupKey,
			uint32(eventRef.timeout), eventRef.expired,
			0, 0)
	}

	// UnlockHead
	err := eventRef.eventLocker.UnlockHead()
	if err.Result != protocol.RESULT_SUCCEED && err.Result != protocol.RESULT_UNLOCK_ERROR {
		return err
	}
	return nil
}

func (eventRef *GroupEvent) isSet() (bool, error) {
	eventRef.checkLocker = BuildNewLock(eventRef.db, eventRef.db.generateLockID(),
		eventRef.groupKey, 0, 0, 0, 0)

	// locker
	err := eventRef.checkLocker.lock()
	if err == nil {
		return true, nil
	}
	if err.Result == protocol.RESULT_TIMEOUT {
		return false, nil
	}
	return false, err
}

func (eventRef *GroupEvent) wakeup() error {
	eventRef.glock.Lock()
	defer eventRef.glock.Unlock()

	lockId := [16]byte{}
	timeout := eventRef.timeout | uint32(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCEED)<<16
	eventRef.eventLocker = BuildNewLock(eventRef.db, lockId,
		eventRef.groupKey,
		uint32(timeout),
		eventRef.expired,
		0, 0)

	err := eventRef.eventLocker.unlockHeadRetoLockWait()
	if err.Result == protocol.RESULT_SUCCEED {
		rlockId := err.CommandResult.LockId
		if rlockId != lockId {
			eventRef.versionId = uint64(rlockId[0]) | uint64(rlockId[1])<<8 | uint64(rlockId[2])<<16 |
				uint64(rlockId[3])<<24 | uint64(rlockId[4])<<32 | uint64(rlockId[5])<<40 |
				uint64(rlockId[6])<<48 | uint64(rlockId[7])<<56
		}
		return nil
	}
	return err
}

func (eventRef *GroupEvent) wait(timeout uint32, generateLockId func(uint64) [16]byte) (bool, error) {
	eventRef.waitLocker = BuildNewLock(eventRef.db, generateLockId(eventRef.versionId), eventRef.groupKey,
		uint32(eventRef.timeout|uint32(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCEED)<<16), 0, 0, 0)
	lockResultCommand, err := eventRef.waitLocker.doLock(0, eventRef.waitLocker.lockId, eventRef.waitLocker.timeout,
		eventRef.waitLocker.expried, eventRef.waitLocker.count, eventRef.waitLocker.rcount)
	if err != nil {
		return false, &LockError{0x80, lockResultCommand, err}
	}
	if lockResultCommand.Result == protocol.RESULT_SUCCEED {
		rlockId := lockResultCommand.LockId
		if rlockId != eventRef.waitLocker.lockId {
			eventRef.versionId = uint64(rlockId[0]) | uint64(rlockId[1])<<8 | uint64(rlockId[2])<<16 |
				uint64(rlockId[3])<<24 | uint64(rlockId[4])<<32 | uint64(rlockId[5])<<40 |
				uint64(rlockId[6])<<48 | uint64(rlockId[7])<<56
		}
		return true, nil
	}
	if lockResultCommand.Result == protocol.RESULT_TIMEOUT {
		return false, nil
	}
	return false, err
}

func (eventRef *GroupEvent) waitAndTimeoutRetryClear(timeout uint32, generateLockId func(uint64) any) (bool, error) {
	lockId := generateLockId(eventRef.versionId)
	timeout = timeout | uint32(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCEED)<<16
	eventRef.waitLocker = BuildNewLock(eventRef.db, lockId, eventRef.groupKey, timeout, 0, 0, 0)
	lockResultCommand, err := eventRef.waitLocker.doLock(0, eventRef.waitLocker.lockId, eventRef.waitLocker.timeout,
		eventRef.waitLocker.expried, eventRef.waitLocker.count, eventRef.waitLocker.rcount)
	if err != nil {
		return false, &LockError{0x80, lockResultCommand, err}
	}
	if lockResultCommand.Result == protocol.RESULT_SUCCEED {
		rlockId := lockResultCommand.LockId
		if rlockId != lockId {
			eventRef.versionId = uint64(rlockId[0]) | uint64(rlockId[1])<<8 | uint64(rlockId[2])<<16 |
				uint64(rlockId[3])<<24 | uint64(rlockId[4])<<32 | uint64(rlockId[5])<<40 |
				uint64(rlockId[6])<<48 | uint64(rlockId[7])<<56
		}
		return true, nil
	}
	if lockResultCommand.Result == protocol.RESULT_TIMEOUT {
		eventRef.glock.Lock()
		defer eventRef.glock.Unlock()
		lockId := generateLockId(eventRef.versionId)
		timeout := eventRef.timeout | uint32(protocol.TIMEOUT_FLAG_LESS_LOCK_VERSION_IS_LOCK_SUCCEED)<<16
		eventRef.eventLocker = BuildNewLock(eventRef.db, lockId, eventRef.groupKey, timeout, eventRef.expired, 0, 0)
		err := eventRef.eventLocker.LockUpdate()
		if err.Result == 0 {
			if err.CommandResult.Result == protocol.RESULT_SUCCEED {
				_ = eventRef.eventLocker.unlock()
				return true, nil
			}
			if err.CommandResult.Result == protocol.RESULT_LOCKED_ERROR {
				return false, nil
			}
		}
	}
	return false, err
}
