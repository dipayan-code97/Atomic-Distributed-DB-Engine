package godev

import (
	"errors"
	"github.com/snower/slock/protocol"
	"sync"
)

type RWLock struct {
	db        *Database
	keyLocker [16]byte
	timeout   uint32
	expired uint32
	rlocks  []*Lock
	wlocker *Lock
	glocker *sync.Mutex
}

func BuildNewRWLock(db *Database, lockKey [16]byte, timeout uint32, expired uint32) *RWLock {
	return &RWLock{db, lockKey, timeout, expired, make([]*Lock, 0), nil, &sync.Mutex{}}
}

func (selfRef *RWLock) getLockKey() [16]byte {
	return selfRef.keyLocker
}

func (selfRef *RWLock) getTimeout() uint32 {
	return selfRef.timeout
}

func (selfRef *RWLock) getExpired() uint32 {
	return selfRef.expired
}

func (selfRef *RWLock) rLock() *LockError {
	rlock := &Lock{selfRef.db, selfRef.db.generateLockID(), selfRef.keyLocker,
				selfRef.timeout, int32(selfRef.expired), 0xffff, 0}
	err := rlock.lock()
	if err == nil {
		selfRef.glocker.Lock()
		selfRef.rlocks = append(selfRef.rlocks, rlock)
		selfRef.glocker.Unlock()
	}
	return err
}

func (selfRef *RWLock) rUnlock() *LockError {
	selfRef.glocker.Lock()
	if len(selfRef.rlocks) == 0 {
		selfRef.glocker.Unlock()
		return &LockError{protocol.RESULT_UNLOCK_ERROR, nil,
					 errors.New("rwlock is unlock")}
	}
	rlock := selfRef.rlocks[0]
	selfRef.rlocks = selfRef.rlocks[1:]
	selfRef.glocker.Unlock()
	return rlock.unlock()
}

func (selfRef *RWLock) lock() *LockError {
	selfRef.glocker.Lock()
	if selfRef.wlocker == nil {
		selfRef.wlocker = &Lock{selfRef.db, selfRef.db.generateLockID(),
						  selfRef.keyLocker, selfRef.timeout,
						  int32(selfRef.expired), 0, 0}
	}
	selfRef.glocker.Unlock()
	return selfRef.wlocker.lock()
}

func (selfRef *RWLock) unlock() *LockError {
	selfRef.glocker.Lock()
	if selfRef.wlocker == nil {
		selfRef.glocker.Unlock()
		return &LockError{protocol.RESULT_UNLOCK_ERROR,
							nil, errors.New("rwlock is unlock")}
	}
	selfRef.glocker.Unlock()
	return selfRef.wlocker.unlock()
}
