package godev

type RLock struct {
	db      *Database
	lockKey [16]byte
	timeout uint32
	expired uint32
	locker  *Lock
}

func BuildNewRLock(db *Database, lockKey [16]byte, timeout int32, expired int32) *RLock {
	lock := &Lock{db, db.generateLockID(), lockKey, uint32(timeout), int32(expired), 0, 0xff}
	return &RLock{db, lockKey, uint32(timeout), uint32(expired), lock}
}

func (selfRef *RLock) getLockKey() [16]byte {
	return selfRef.lockKey
}

func (selfRef *RLock) getTimeout() uint32 {
	return selfRef.timeout
}

func (selfRef *RLock) getExpired() uint32 {
	return selfRef.expired
}

func (selfRef *RLock) lock() *Lock {
	return selfRef.locker
}

func (selfRef *RLock) unlock() *Lock {
	return selfRef.locker
}
