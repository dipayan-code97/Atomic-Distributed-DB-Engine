package godev

import (
	"github.com/snower/slock/protocol"
	"math"
	"sync"
	"time"
)

type MaxConcurrentFlow struct {
	db       *Database
	flowKey  [16]byte
	count    uint16
	timeout  uint32
	expired  uint32
	flowLock *Lock
	glock    *sync.Mutex
}

func BuildNewMaxConcurrentFlow(db *Database, flowKey [16]byte, count uint16,
	timeout uint32, expired uint32) *MaxConcurrentFlow {
	if count > 0 {
		count -= 1
	}
	return &MaxConcurrentFlow{db, flowKey, count,
		timeout, expired, nil, &sync.Mutex{}}
}

func (selfRef *MaxConcurrentFlow) getTimeoutFlag() uint16 {
	return uint16((selfRef.timeout & 0xffff) >> 16)
}

func (selfRef *MaxConcurrentFlow) setTimeoutFlag(flag uint16) uint16 {
	oflag := selfRef.getTimeoutFlag()
	selfRef.timeout = (selfRef.timeout & 0xffff) | (uint32(flag) << 16)
	return oflag
}

func (selfRef *MaxConcurrentFlow) getExpiredFlag() uint16 {
	return uint16((selfRef.expired & 0xffff) >> 16)
}

func (selfRef *MaxConcurrentFlow) setExpiredFlag(flag uint16) uint16 {
	oflag := selfRef.getExpiredFlag()
	selfRef.expired = (selfRef.expired & 0xffff) | (uint32(flag) << 16)
	return oflag
}

func (selfRef *MaxConcurrentFlow) acquire() *LockError {
	selfRef.glock.Lock()
	if selfRef.flowLock == nil {
		selfRef.flowLock = &Lock{selfRef.db, selfRef.db.generateLockID(),
			selfRef.flowKey, selfRef.timeout,
			int32(selfRef.expired), selfRef.count, 0}
	}
	selfRef.glock.Unlock()
	return selfRef.flowLock.lock()
}

func (selfRef *MaxConcurrentFlow) release() *LockError {
	selfRef.glock.Lock()
	if selfRef.flowLock == nil {
		selfRef.flowLock = &Lock{selfRef.db, selfRef.db.generateLockID(),
			selfRef.flowKey, selfRef.timeout,
			int32(selfRef.expired), selfRef.count, 0}
	}
	selfRef.glock.Unlock()
	return selfRef.flowLock.unlock()
}

type TokenBucketFlow struct {
	db          *Database
	flowKey     [16]byte
	count       uint16
	timeout     uint32
	period      float64
	expriedFlag uint16
	flowLock    *Lock
	glock       *sync.Mutex
}

func BuildNewTokenBucketFlow(db *Database, flowKey [16]byte, count uint16, timeout uint32, period float64) *TokenBucketFlow {
	if count > 0 {
		count -= 1
	}
	return &TokenBucketFlow{db, flowKey, count, timeout, period, 0, nil, &sync.Mutex{}}
}

func (selfRef *TokenBucketFlow) getTimeoutFlag() uint16 {
	return uint16((selfRef.timeout & 0xffff) >> 16)
}

func (selfRef *TokenBucketFlow) setTimeoutFlag(flag uint16) uint16 {
	oflag := selfRef.getTimeoutFlag()
	selfRef.timeout = (selfRef.timeout & 0xffff) | (uint32(flag) << 16)
	return oflag
}

func (selfRef *TokenBucketFlow) getExpiredFlag() uint16 {
	return selfRef.expriedFlag
}

func (selfRef *TokenBucketFlow) setExpiredFlag(flag uint16) uint16 {
	oflag := selfRef.getExpiredFlag()
	selfRef.expriedFlag = flag
	return oflag
}

func (selfRef *TokenBucketFlow) acquire() *LockError {
	selfRef.glock.Lock()
	if selfRef.period < 3 {
		expired := uint32(math.Ceil(selfRef.period*1000)) | 0x04000000
		expired |= uint32(selfRef.expriedFlag) << 16
		selfRef.flowLock = &Lock{selfRef.db, selfRef.db.generateLockID(),
			selfRef.flowKey, selfRef.timeout, int32(expired), selfRef.count, 0}
		selfRef.glock.Unlock()
		return selfRef.flowLock.lock()
	}

	now := time.Now().UnixNano() / 1e9
	expired := uint32(int64(math.Ceil(selfRef.period)) - (now % int64(math.Ceil(selfRef.period))))
	expired |= uint32(selfRef.expriedFlag) << 16
	selfRef.flowLock = &Lock{selfRef.db, selfRef.db.generateLockID(),
		selfRef.flowKey, 0, int32(expired), selfRef.count, 0}
	selfRef.glock.Unlock()

	err := selfRef.flowLock.lock()
	if err != nil && err.Result == protocol.RESULT_TIMEOUT {
		selfRef.glock.Lock()
		expired := uint32(math.Ceil(selfRef.period))
		expired |= uint32(selfRef.expriedFlag) << 16
		selfRef.flowLock = &Lock{selfRef.db, selfRef.db.generateLockID(),
			selfRef.flowKey, selfRef.timeout,
			int32(expired), selfRef.count, 0}
		selfRef.glock.Unlock()
		return selfRef.flowLock.lock()
	}
	return err
}
