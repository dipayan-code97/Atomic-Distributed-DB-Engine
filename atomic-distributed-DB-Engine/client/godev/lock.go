package godev

import (
	"errors"
	"fmt"
	"github.com/snower/slock/protocol"
)

type LockError struct {
	Result        uint8                       `json:"result,omitempty"`
	CommandResult *protocol.LockResultCommand `json:"commandResult,omitempty"`
	Err           error                       `json:"err,omitempty"`
}

func (selfRef *LockError) error() string {
	if selfRef.Err == nil {
		return fmt.Sprintf("error code %d", selfRef.Result)
	}
	return fmt.Sprintf("%d %s", selfRef.Result, selfRef.Err.Error())
}

type Lock struct {
	db      *Database `json:"db,omitempty"`
	lockId  [16]byte  `json:"lockId,omitempty"`
	lockKey [16]byte  `json:"keyLocker,omitempty"`
	timeout uint32    `json:"timeout,omitempty"`
	expired int32     `json:"expired,omitempty"`
	count   uint16    `json:"count,omitempty"`
	rcount  uint8     `json:"rcount,omitempty"`
}

func (selfRef *Lock) getLockKey() [16]byte {
	return selfRef.lockKey
}

func (selfRef *Lock) getLockId() [16]byte {
	return selfRef.lockId
}

func (selfRef *Lock) getTimeout() uint16 {
	return uint16(selfRef.timeout & 0xffff)
}

func (selfRef *Lock) getTimeoutFlag() uint16 {
	return uint16(selfRef.timeout >> 16)
}

func (selfRef *Lock) setTimeoutFlag(flag uint16) uint16 {
	oflag := selfRef.getTimeoutFlag()
	selfRef.timeout = (selfRef.timeout & 0xffff) | (uint32(flag) << 16)
	return oflag
}

func (selfRef *Lock) getExpired() uint16 {
	return uint16(selfRef.expired & 0xffff)
}

func (selfRef *Lock) getExpiredFlag() uint16 {
	return uint16(selfRef.expired >> 16)
}

func (selfRef *Lock) setExpiredFlag(flag uint16) uint16 {
	oflag := selfRef.getExpiredFlag()
	selfRef.expired = (selfRef.expired & 0xffff) | (uint32(flag) << 16)
	return oflag
}

func (selfRef *Lock) GetCount() uint16 {
	return selfRef.count
}

func (selfRef *Lock) SetCount(count uint16) uint16 {
	ocount := selfRef.count
	if count > 0 {
		selfRef.count = count - 1
	} else {
		selfRef.count = 0
	}
	return ocount
}

func (selfRef *Lock) getRcount() uint8 {
	return selfRef.rcount
}

func (selfRef *Lock) setRcount(rcount uint8) uint8 {
	orcount := selfRef.rcount
	if rcount > 0 {
		selfRef.rcount = rcount - 1
	} else {
		selfRef.rcount = 0
	}
	return orcount
}

func (selfRef *Lock) doLock(flag uint8, lockId [16]byte, timeout uint32,
	expired int32, count uint16, rcount uint8) (*protocol.LockResultCommand, error) {
	command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC,
		Version: protocol.VERSION, CommandType: protocol.COMMAND_LOCK,
		RequestId: selfRef.db.generateRequestID()},
		Flag: flag, DbId: selfRef.db.dbId, LockId: lockId, LockKey: selfRef.lockKey, TimeoutFlag: uint16(timeout >> 16), Timeout: uint16(timeout),
		ExpriedFlag: uint16(expired >> 16), Expried: uint16(expired), Count: count, Rcount: rcount}
	resultCommand, err := selfRef.db.executeCommand(command, int(command.Timeout+1))
	if err != nil {
		return nil, err
	}

	lockResultCommand, ok := resultCommand.(*protocol.LockResultCommand)
	if !ok {
		return nil, errors.New("unknown command result")
	}
	return lockResultCommand, nil
}

func (selfRef *Lock) doUnlock(flag uint8, lockId [16]byte, timeout uint32, expired int32,
	count uint16, rcount uint8) (*protocol.LockResultCommand, error) {
	command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC,
		Version: protocol.VERSION, CommandType: protocol.COMMAND_UNLOCK,
		RequestId: selfRef.db.generateRequestID()},
		Flag: flag, DbId: selfRef.db.dbId, LockId: lockId, LockKey: selfRef.lockKey,
		TimeoutFlag: uint16(timeout >> 16), Timeout: uint16(timeout),
		ExpriedFlag: uint16(expired >> 16), Expried: uint16(expired), Count: count, Rcount: rcount}
	resultCommand, err := selfRef.db.executeCommand(command, int(command.Timeout+1))
	if err != nil {
		return nil, err
	}

	lockResultCommand, ok := resultCommand.(*protocol.LockResultCommand)
	if !ok {
		return nil, errors.New("unknown command result")
	}
	return lockResultCommand, nil
}

func (selfRef *Lock) lock() *LockError {
	lockResultCommand, err := selfRef.doLock(0, selfRef.lockId, selfRef.timeout, selfRef.expired, selfRef.count, selfRef.rcount)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result != 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return nil
}

func (selfRef *Lock) unlock() *LockError {
	lockResultCommand, err := selfRef.doUnlock(0, selfRef.lockId,
		selfRef.timeout, selfRef.expired, selfRef.count, selfRef.rcount)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result != 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, err}
	}
	return nil
}

func (selfRef *Lock) lockShow() *LockError {
	lockResultCommand, err := selfRef.doLock(protocol.LOCK_FLAG_SHOW_WHEN_LOCKED, [16]byte{}, 0, 0, 0xffff, 0xff)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == protocol.RESULT_UNOWN_ERROR {
		return &LockError{0, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("show error")}
}

func (selfRef *Lock) lockUpdate() *LockError {
	lockResultCommand, err := selfRef.doLock(protocol.LOCK_FLAG_UPDATE_WHEN_LOCKED, selfRef.lockId, selfRef.timeout,
		selfRef.expired, selfRef.count, selfRef.rcount)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == 0 || lockResultCommand.Result == protocol.RESULT_LOCKED_ERROR {
		return &LockError{0, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("update error")}
}

func (selfRef *Lock) unlockHead() *LockError {
	lockResultCommand, err := selfRef.doUnlock(protocol.UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED, [16]byte{},
		selfRef.timeout, selfRef.expired, selfRef.count, selfRef.rcount)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("unlock error")}
}

func (selfRef *Lock) unlockRetoLockWait() *LockError {
	lockResultCommand, err := selfRef.doUnlock(protocol.UNLOCK_FLAG_SUCCEED_TO_LOCK_WAIT, selfRef.lockId,
		selfRef.timeout, selfRef.expired, selfRef.count, selfRef.rcount)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("unlock error")}
}

func (selfRef *Lock) unlockHeadRetoLockWait() *LockError {
	lockResultCommand, err := selfRef.doUnlock(protocol.UNLOCK_FLAG_UNLOCK_FIRST_LOCK_WHEN_UNLOCKED|protocol.UNLOCK_FLAG_SUCCEED_TO_LOCK_WAIT, [16]byte{},
		selfRef.timeout, selfRef.expired, selfRef.count, selfRef.rcount)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == 0 {
		return &LockError{lockResultCommand.Result, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("unlock error")}
}

func (selfRef *Lock) cancelWait() *LockError {
	lockResultCommand, err := selfRef.doUnlock(protocol.UNLOCK_FLAG_CANCEL_WAIT_LOCK_WHEN_UNLOCKED, selfRef.lockId,
		selfRef.timeout, selfRef.expired, selfRef.count, selfRef.rcount)
	if err != nil {
		return &LockError{0x80, lockResultCommand, err}
	}

	if lockResultCommand.Result == protocol.RESULT_LOCKED_ERROR {
		return &LockError{0, lockResultCommand, nil}
	}
	return &LockError{lockResultCommand.Result, lockResultCommand, errors.New("cancel error")}
}

func (selfRef *Lock) sendLock() error {
	command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION,
		CommandType: protocol.COMMAND_LOCK,
		RequestId:   selfRef.db.generateRequestID()},
		Flag: 0, DbId: selfRef.db.dbId, LockId: selfRef.lockId, LockKey: selfRef.lockKey, TimeoutFlag: uint16(selfRef.timeout >> 16), Timeout: uint16(selfRef.timeout),
		ExpriedFlag: uint16(selfRef.expired >> 16), Expried: uint16(selfRef.expired), Count: selfRef.count, Rcount: selfRef.rcount}
	return selfRef.db.sendCommand(command)
}

func (selfRef *Lock) sendUnlock() error {
	command := &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC,
		Version: protocol.VERSION, CommandType: protocol.COMMAND_UNLOCK, RequestId: selfRef.db.generateRequestID()},
		Flag: 0, DbId: selfRef.db.dbId, LockId: selfRef.lockId, LockKey: selfRef.lockKey, TimeoutFlag: uint16(selfRef.timeout >> 16), Timeout: uint16(selfRef.timeout),
		ExpriedFlag: uint16(selfRef.expired >> 16), Expried: uint16(selfRef.expired), Count: selfRef.count, Rcount: selfRef.rcount}
	return selfRef.db.sendCommand(command)
}
