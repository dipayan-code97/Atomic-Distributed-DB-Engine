package godev

import (
	"errors"
	"fmt"
	"github.com/snower/slock/protocol"
	"github.com/snower/slock/protocol/protobuf"
	"google.golang.org/protobuf/proto"
	"sync"
	"time"
)

type Database struct {
	dbId   uint8       `json:"dbId,omitempty"`
	client IClient     `json:"client,omitempty"`
	glock  *sync.Mutex `json:"glocker,omitempty"`
	closed bool        `json:"closed,omitempty"`
}

func BuildNewDatabase(dbId uint8, client *ReplsetClient) *Database {
	return &Database{dbId, client, &sync.Mutex{}, false}
}

func (selfRef *Database) close() error {
	selfRef.closed = true
	return nil
}

func (selfRef *Database) executeCommand(command protocol.ICommand, timeout int) (protocol.ICommand, error) {
	if selfRef.client == nil {
		return nil, errors.New("db is not closed")
	}
	return selfRef.client.executeCommand(command, timeout)
}

func (selfRef *Database) sendCommand(command protocol.ICommand) error {
	if selfRef.client == nil {
		return errors.New("db is not closed")
	}
	return selfRef.client.sendCommand(command)
}

func (selfRef *Database) lock(lockKey [16]byte, timeout, expired uint32) *Lock {
	return BuildNewLock(selfRef, lockKey,, 0, 0, timeout, expired)
}

func (selfRef *Database) event(eventKey [16]byte, timeout time.Duration, expired uint32, defaultSeted bool) *Event {
	if defaultSeted {
		return NewDefaultSetEvent(selfRef, eventKey, timeout, expired)
	}
	return NewDefaultClearEvent(selfRef, eventKey, timeout, expired)
}

func (selfRef *Database) groupEvent(groupKey [16]byte, clientId uint64,
	versionId uint64, timeout time.Duration,
	expired uint32) *GroupEvent {
	return BuildNewGroupEvent(selfRef, groupKey, clientId, versionId, timeout, expired)
}

func (selfRef *Database) semaphore(semaphoreKey [16]byte,
	timeout time.Duration,
	expired uint32,
	count uint16) *Semaphore {
	return NewSemaphore(selfRef, semaphoreKey, timeout, expired, count)
}

func (selfRef *Database) rwLock(lockKey [16]byte,
	timeout time.Duration,
	expired uint32) *RWLock {
	return BuildNewRWLock(selfRef, lockKey, timeout, expired)
}

func (selfRef *Database) rLock(lockKey [16]byte, timeout time.Duration, expired uint32) *RLock {
	return BuildNewRLock(selfRef, lockKey, timeout, expired)
}

func (selfRef *Database) maxConcurrentFlow(flowKey [16]byte, count uint16,
	timeout time.Duration, expired uint32) *MaxConcurrentFlow {
	return BuildNewMaxConcurrentFlow(selfRef, flowKey, count, timeout, expired)
}

func (selfRef *Database) tokenBucketFlow(flowKey [16]byte, count uint16,
	timeout time.Duration, period float64) *TokenBucketFlow {
	return BuildNewTokenBucketFlow(selfRef, flowKey, count, timeout, period)
}

func (selfRef *Database) treeLock(lockKey [16]byte, parentKey [16]byte, timeout uint32, expried uint32) *TreeLock {
	return NewTreeLock(selfRef, lockKey, parentKey, timeout, expried)
}

func (selfRef *Database) State() *protocol.StateResultCommand {
	requestId := selfRef.client.generateRequestId()
	command := &protocol.StateCommand{Command: protocol.Command{Magic: protocol.MAGIC,
		Version:     protocol.VERSION,
		CommandType: protocol.CommandState,
		RequestId:   requestId},
		Flag: 0, DbId: selfRef.dbId, Blank: [43]byte{}}
	resultCommand, err := selfRef.executeCommand(command, 5)
	if err != nil {
		return nil
	}

	if stateResultCommand, ok := resultCommand.(*protocol.StateResultCommand); ok {
		return stateResultCommand
	}
	return nil
}

func (selfRef *Database) listLocks(timeout time.Duration) (*protobuf.LockDBListLockResponse, error) {
	requestRegistry := protobuf.LockDBListLockRequest{DbId: uint32(selfRef.dbId)}
	data, err := proto.Marshal(&requestRegistry)
	if err != nil {
		return nil, err
	}

	command := protocol.NewCallCommand("LIST_LOCK", data)
	resultCommand, err := selfRef.client.executeCommand(command, timeout)
	if err != nil {
		return nil, err
	}

	callResultCommand := resultCommand.(*protocol.CallResultCommand)
	if callResultCommand.Result != protocol.RESULT_SUCCEED {
		return nil, errors.New(fmt.Sprintf("call error: error code %d", callResultCommand.Result))
	}

	response := protobuf.LockDBListLockResponse{}
	err = proto.Unmarshal(callResultCommand.Data, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (selfRef *Database) listLockLocked(lockKey [16]byte, timeout time.Duration) (*protobuf.LockDBListLockedResponse, error) {
	request := protobuf.LockDBListLockedRequest{DbId: uint32(selfRef.dbId), LockKey: lockKey[:]}
	data, err := proto.Marshal(&request)
	if err != nil {
		return nil, err
	}

	command := protocol.NewCallCommand("LIST_LOCKED", data)
	resultCommand, err := selfRef.client.executeCommand(command, timeout)
	if err != nil {
		return nil, err
	}

	callResultCommand := resultCommand.(*protocol.CallResultCommand)
	if callResultCommand.Result != protocol.RESULT_SUCCEED {
		return nil, errors.New(fmt.Sprintf("call error: error code %d", callResultCommand.Result))
	}

	response := protobuf.LockDBListLockedResponse{}
	err = proto.Unmarshal(callResultCommand.Data, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (selfRef *Database) listLockWaits(lockKey [16]byte, timeout time.Duration) (*protobuf.LockDBListWaitResponse, error) {
	request := protobuf.LockDBListWaitRequest{DbId: uint32(selfRef.dbId), LockKey: lockKey[:]}
	data, err := proto.Marshal(&request)
	if err != nil {
		return nil, err
	}

	command := protocol.NewCallCommand("LIST_WAIT", data)
	resultCommand, err := selfRef.client.executeCommand(command, timeout)
	if err != nil {
		return nil, err
	}

	callResultCommand := resultCommand.(*protocol.CallResultCommand)
	if callResultCommand.Result != protocol.RESULT_SUCCEED {
		return nil, errors.New(fmt.Sprintf("call error: error code %d", callResultCommand.Result))
	}

	response := protobuf.LockDBListWaitResponse{}
	err = proto.Unmarshal(callResultCommand.Data, &response)
	if err != nil {
		return nil, err
	}
	return &response, nil
}

func (selfRef *Database) generateRequestID() [16]byte {
	if selfRef.client == nil {
		return [16]byte{}
	}
	return selfRef.client.generateRequestId()
}

func (selfRef *Database) generateLockID() [16]byte {
	return protocol.GenLockId()
}
