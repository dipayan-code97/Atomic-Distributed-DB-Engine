package server

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/snower/slock/protocol"
	"github.com/snower/slock/protocol/protobuf"
	"google.golang.org/protobuf/proto"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
)

type ServerProtocol interface {
	Init(clientId [16]byte) error
	Lock()
	Unlock()
	Read() (protocol.CommandDecode, error)
	Write(protocol.CommandEncode) error
	ReadCommand() (protocol.CommandDecode, error)
	WriteCommand(protocol.CommandEncode) error
	Process() error
	ProcessParse(buf []byte) error
	ProcessBuild(command protocol.ICommand) error
	ProcessCommad(command protocol.ICommand) error
	ProcessLockCommand(command *protocol.LockCommand) error
	ProcessLockResultCommand(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error
	ProcessLockResultCommandLocked(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error
	Close() error
	GetStream() *Stream
	GetProxy() *ServerProtocolProxy
	AddProxy(proxy *ServerProtocolProxy) error
	RemoteAddr() net.Addr
	GetLockCommand() *protocol.LockCommand
	FreeLockCommand(command *protocol.LockCommand) error
	FreeLockCommandLocked(command *protocol.LockCommand) error
}

var AGAIN = errors.New("AGAIN")
var serverProtocolSessionIdIndex uint32 = 0

type ServerProtocolSession struct {
	sessionId         uint32
	serverProtocol    ServerProtocol
	totalCommandCount uint64
}

type ServerProtocolProxy struct {
	clientId       [16]byte
	serverProtocol ServerProtocol
}

func (self *ServerProtocolProxy) ProcessLockResultCommandLocked(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	if self.serverProtocol == defaultServerProtocol {
		defaultServerProtocol.slock.clientsGlock.Lock()
		if self.serverProtocol == defaultServerProtocol {
			if serverProtocol, ok := defaultServerProtocol.slock.clients[self.clientId]; ok {
				defaultServerProtocol.slock.clientsGlock.Unlock()
				err := serverProtocol.AddProxy(self)
				if err == nil {
					self.serverProtocol = serverProtocol
				}
				return serverProtocol.ProcessLockResultCommandLocked(command, result, lcount, lrcount)
			}
			defaultServerProtocol.slock.clientsGlock.Unlock()
			return errors.New("Protocol Closed")
		}
		defaultServerProtocol.slock.clientsGlock.Unlock()
	}
	return self.serverProtocol.ProcessLockResultCommandLocked(command, result, lcount, lrcount)
}

func (self *ServerProtocolProxy) FreeLockCommandLocked(command *protocol.LockCommand) error {
	return self.serverProtocol.FreeLockCommandLocked(command)
}

func (self *ServerProtocolProxy) GetStream() *Stream {
	if self.serverProtocol != nil {
		return self.serverProtocol.GetStream()
	}
	return nil
}

func (self *ServerProtocolProxy) RemoteAddr() net.Addr {
	if self.serverProtocol != nil {
		return self.serverProtocol.RemoteAddr()
	}
	return &net.TCPAddr{IP: []byte("0.0.0.0"), Port: 0, Zone: ""}
}

type DefaultServerProtocol struct {
	slock         *SLock
	protocolProxy *ServerProtocolProxy
}

func NewDefaultServerProtocol(slock *SLock) *DefaultServerProtocol {
	proxy := &ServerProtocolProxy{[16]byte{}, nil}
	serverProtocol := &DefaultServerProtocol{slock, proxy}
	proxy.serverProtocol = serverProtocol
	return serverProtocol
}

func (self *DefaultServerProtocol) Init(_ [16]byte) error {
	return nil
}

func (self *DefaultServerProtocol) Lock() {

}

func (self *DefaultServerProtocol) Unlock() {

}

func (self *DefaultServerProtocol) Read() (protocol.CommandDecode, error) {
	return nil, errors.New("not support")
}

func (self *DefaultServerProtocol) Write(protocol.CommandEncode) error {
	return errors.New("not support")
}

func (self *DefaultServerProtocol) ReadCommand() (protocol.CommandDecode, error) {
	return nil, errors.New("not support")
}

func (self *DefaultServerProtocol) WriteCommand(protocol.CommandEncode) error {
	return errors.New("not support")
}

func (self *DefaultServerProtocol) Process() error {
	return io.EOF
}

func (self *DefaultServerProtocol) ProcessParse(_ []byte) error {
	return io.EOF
}

func (self *DefaultServerProtocol) ProcessBuild(_ protocol.ICommand) error {
	return nil
}

func (self *DefaultServerProtocol) ProcessCommad(command protocol.ICommand) error {
	switch command.GetCommandType() {
	case protocol.COMMAND_LOCK:
		lockCommand := command.(*protocol.LockCommand)

		if lockCommand.DbId == 0xff {
			err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.FreeLockCommand(lockCommand)
			return err
		}

		db := self.slock.dbs[lockCommand.DbId]
		if db == nil {
			db = self.slock.GetOrNewDB(lockCommand.DbId)
		}
		return db.Lock(self, lockCommand)

	case protocol.COMMAND_UNLOCK:
		lockCommand := command.(*protocol.LockCommand)

		if lockCommand.DbId == 0xff {
			err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.FreeLockCommand(lockCommand)
			return err
		}

		db := self.slock.dbs[lockCommand.DbId]
		if db == nil {
			err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.FreeLockCommand(lockCommand)
			return err
		}
		return db.UnLock(self, lockCommand)

	default:
		return self.Write(protocol.NewResultCommand(command, protocol.RESULT_UNKNOWN_COMMAND))
	}
}

func (self *DefaultServerProtocol) ProcessLockCommand(lockCommand *protocol.LockCommand) error {
	if lockCommand.DbId == 0xff {
		err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
		_ = self.FreeLockCommand(lockCommand)
		return err
	}

	db := self.slock.dbs[lockCommand.DbId]
	if lockCommand.CommandType == protocol.COMMAND_LOCK {
		if db == nil {
			db = self.slock.GetOrNewDB(lockCommand.DbId)
		}
		return db.Lock(self, lockCommand)
	}

	if db == nil {
		err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
		_ = self.FreeLockCommand(lockCommand)
		return err
	}
	return db.UnLock(self, lockCommand)
}

func (self *DefaultServerProtocol) ProcessLockResultCommand(_ *protocol.LockCommand, _ uint8, _ uint16, _ uint8) error {
	return nil
}

func (self *DefaultServerProtocol) ProcessLockResultCommandLocked(_ *protocol.LockCommand, _ uint8, _ uint16, _ uint8) error {
	return nil
}

func (self *DefaultServerProtocol) Close() error {
	return nil
}

func (self *DefaultServerProtocol) GetStream() *Stream {
	return nil
}

func (self *DefaultServerProtocol) GetProxy() *ServerProtocolProxy {
	return self.protocolProxy
}

func (self *DefaultServerProtocol) AddProxy(_ *ServerProtocolProxy) error {
	return nil
}

func (self *DefaultServerProtocol) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: []byte("0.0.0.0"), Port: 0, Zone: ""}
}

func (self *DefaultServerProtocol) GetLockCommand() *protocol.LockCommand {
	self.slock.freeLockCommandLock.Lock()
	lockCommand := self.slock.freeLockCommandQueue.PopRight()
	if lockCommand != nil {
		self.slock.freeLockCommandCount--
		self.slock.freeLockCommandLock.Unlock()
		return lockCommand
	}
	self.slock.freeLockCommandLock.Unlock()
	return &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION}}
}

func (self *DefaultServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
	return self.FreeLockCommandLocked(command)
}

func (self *DefaultServerProtocol) FreeLockCommandLocked(command *protocol.LockCommand) error {
	self.slock.freeLockCommandLock.Lock()
	_ = self.slock.freeLockCommandQueue.Push(command)
	self.slock.freeLockCommandCount++
	self.slock.freeLockCommandLock.Unlock()
	return nil
}

var defaultServerProtocol *DefaultServerProtocol = nil

type MemWaiterServerProtocolResultCallback func(*MemWaiterServerProtocol, *protocol.LockCommand, uint8, uint16, uint8) error

type MemWaiterServerProtocol struct {
	slock              *SLock
	glock              *sync.Mutex
	session            *ServerProtocolSession
	proxys             []*ServerProtocolProxy
	freeCommands       []*protocol.LockCommand
	freeCommandIndex   int
	lockedFreeCommands *LockCommandQueue
	waiters            map[[16]byte]chan *protocol.LockResultCommand
	resultCallback     MemWaiterServerProtocolResultCallback
	totalCommandCount  uint64
	closed             bool
}

func NewMemWaiterServerProtocol(slock *SLock) *MemWaiterServerProtocol {
	proxy := &ServerProtocolProxy{[16]byte{}, nil}
	memWaiterServerProtocol := &MemWaiterServerProtocol{slock, &sync.Mutex{}, nil, make([]*ServerProtocolProxy, 0), make([]*protocol.LockCommand, FREE_COMMAND_MAX_SIZE),
		0, NewLockCommandQueue(4, 64, FREE_COMMAND_QUEUE_INIT_SIZE),
		make(map[[16]byte]chan *protocol.LockResultCommand, 4096), nil, 0, false}
	proxy.serverProtocol = memWaiterServerProtocol
	memWaiterServerProtocol.InitLockCommand()
	memWaiterServerProtocol.session = slock.addServerProtocol(memWaiterServerProtocol)
	_ = memWaiterServerProtocol.AddProxy(proxy)
	return memWaiterServerProtocol
}

func (self *MemWaiterServerProtocol) Init(_ [16]byte) error {
	return nil
}

func (self *MemWaiterServerProtocol) Lock() {
	self.glock.Lock()
}

func (self *MemWaiterServerProtocol) Unlock() {
	self.glock.Unlock()
}

func (self *MemWaiterServerProtocol) Read() (protocol.CommandDecode, error) {
	return nil, errors.New("read error")
}

func (self *MemWaiterServerProtocol) Write(protocol.CommandEncode) error {
	return errors.New("write error")
}

func (self *MemWaiterServerProtocol) ReadCommand() (protocol.CommandDecode, error) {
	return nil, errors.New("read error")
}

func (self *MemWaiterServerProtocol) WriteCommand(protocol.CommandEncode) error {
	return errors.New("write error")
}

func (self *MemWaiterServerProtocol) Process() error {
	return nil
}

func (self *MemWaiterServerProtocol) ProcessParse(_ []byte) error {
	return nil
}

func (self *MemWaiterServerProtocol) ProcessBuild(_ protocol.ICommand) error {
	return nil
}

func (self *MemWaiterServerProtocol) ProcessCommad(_ protocol.ICommand) error {
	return nil
}

func (self *MemWaiterServerProtocol) ProcessLockCommand(lockCommand *protocol.LockCommand) error {
	db := self.slock.dbs[lockCommand.DbId]
	self.totalCommandCount++
	switch lockCommand.CommandType {
	case protocol.COMMAND_LOCK:
		if db == nil {
			db = self.slock.GetOrNewDB(lockCommand.DbId)
		}
		return db.Lock(self, lockCommand)
	case protocol.COMMAND_UNLOCK:
		if db == nil {
			err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.FreeLockCommand(lockCommand)
			return err
		}
		return db.UnLock(self, lockCommand)
	}
	return self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_COMMAND, 0, 0)
}

func (self *MemWaiterServerProtocol) ProcessLockResultCommand(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	if self.resultCallback != nil {
		return self.resultCallback(self, command, result, lcount, lrcount)
	}

	self.glock.Lock()
	if waiter, ok := self.waiters[command.RequestId]; ok {
		waiter <- protocol.NewLockResultCommand(command, result, 0, lcount, command.Count, lrcount, command.Rcount)
		delete(self.waiters, command.RequestId)
	}
	self.glock.Unlock()
	return nil
}

func (self *MemWaiterServerProtocol) ProcessLockResultCommandLocked(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	return self.ProcessLockResultCommand(command, result, lcount, lrcount)
}

func (self *MemWaiterServerProtocol) Close() error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return nil
	}

	self.closed = true
	for _, proxy := range self.proxys {
		proxy.serverProtocol = defaultServerProtocol
	}
	self.proxys = self.proxys[:1]
	_ = self.slock.removeServerProtocol(self.session)
	self.glock.Unlock()

	self.slock.clientsGlock.Lock()
	self.slock.statsTotalCommandCount += self.totalCommandCount
	self.slock.clientsGlock.Unlock()

	self.glock.Lock()
	self.UnInitLockCommand()
	self.session = nil
	self.glock.Unlock()
	return nil
}

func (self *MemWaiterServerProtocol) GetStream() *Stream {
	return nil
}

func (self *MemWaiterServerProtocol) GetProxy() *ServerProtocolProxy {
	return self.proxys[0]
}

func (self *MemWaiterServerProtocol) AddProxy(proxy *ServerProtocolProxy) error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return errors.New("closed")
	}

	self.proxys = append(self.proxys, proxy)
	self.glock.Unlock()
	return nil
}

func (self *MemWaiterServerProtocol) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: []byte("0.0.0.0"), Port: 0, Zone: ""}
}

func (self *MemWaiterServerProtocol) InitLockCommand() {
	self.slock.freeLockCommandLock.Lock()
	lockCommand := self.slock.freeLockCommandQueue.PopRight()
	if lockCommand != nil {
		self.slock.freeLockCommandCount--
		self.freeCommands[self.freeCommandIndex] = lockCommand
		self.freeCommandIndex++
	}
	self.slock.freeLockCommandLock.Unlock()
}

func (self *MemWaiterServerProtocol) UnInitLockCommand() {
	self.slock.freeLockCommandLock.Lock()
	for self.freeCommandIndex > 0 {
		self.freeCommandIndex--
		_ = self.slock.freeLockCommandQueue.Push(self.freeCommands[self.freeCommandIndex])
		self.slock.freeLockCommandCount++
	}

	for {
		command := self.lockedFreeCommands.PopRight()
		if command == nil {
			break
		}
		_ = self.slock.freeLockCommandQueue.Push(command)
		self.slock.freeLockCommandCount++
	}
	self.slock.freeLockCommandLock.Unlock()
}

func (self *MemWaiterServerProtocol) GetLockCommand() *protocol.LockCommand {
	if self.freeCommandIndex > 0 {
		self.freeCommandIndex--
		return self.freeCommands[self.freeCommandIndex]
	}

	lockCommand := self.lockedFreeCommands.PopRight()
	if lockCommand != nil {
		return lockCommand
	}

	self.slock.freeLockCommandLock.Lock()
	lockCommand = self.slock.freeLockCommandQueue.PopRight()
	if lockCommand != nil {
		self.slock.freeLockCommandCount--
		self.slock.freeLockCommandLock.Unlock()
		return lockCommand
	}
	self.slock.freeLockCommandLock.Unlock()
	return &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION}}

}

func (self *MemWaiterServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
	if self.freeCommandIndex < FREE_COMMAND_MAX_SIZE {
		self.freeCommands[self.freeCommandIndex] = command
		self.freeCommandIndex++
		return nil
	}
	return self.FreeLockCommandLocked(command)
}

func (self *MemWaiterServerProtocol) FreeLockCommandLocked(command *protocol.LockCommand) error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		self.slock.freeLockCommandLock.Lock()
		_ = self.slock.freeLockCommandQueue.Push(command)
		self.slock.freeLockCommandCount++
		self.slock.freeLockCommandLock.Unlock()
		return nil
	}

	_ = self.lockedFreeCommands.Push(command)
	self.glock.Unlock()
	return nil
}

func (self *MemWaiterServerProtocol) AddWaiter(command *protocol.LockCommand, waiter chan *protocol.LockResultCommand) error {
	self.glock.Lock()
	if owaiter, ok := self.waiters[command.RequestId]; ok {
		owaiter <- nil
	}
	self.waiters[command.RequestId] = waiter
	self.glock.Unlock()
	return nil
}

func (self *MemWaiterServerProtocol) RemoveWaiter(command *protocol.LockCommand) error {
	self.glock.Lock()
	if _, ok := self.waiters[command.RequestId]; ok {
		delete(self.waiters, command.RequestId)
	}
	self.glock.Unlock()
	return nil
}

func (self *MemWaiterServerProtocol) SetResultCallback(callback MemWaiterServerProtocolResultCallback) error {
	self.resultCallback = callback
	return nil
}

type BinaryServerProtocolCallHandler func(*BinaryServerProtocol, *protocol.CallCommand) (*protocol.CallResultCommand, error)

type BinaryServerProtocol struct {
	slock              *SLock
	glock              *sync.Mutex
	stream             *Stream
	session            *ServerProtocolSession
	proxys             []*ServerProtocolProxy
	freeCommands       []*protocol.LockCommand
	freeCommandIndex   int
	lockedFreeCommands *LockCommandQueue
	rbuf               []byte
	wbuf               []byte
	rindex             int
	rlen               int
	callMethods        map[string]BinaryServerProtocolCallHandler
	willCommands       *LockCommandQueue
	totalCommandCount  uint64
	inited             bool
	closed             bool
}

func NewBinaryServerProtocol(slock *SLock, stream *Stream) *BinaryServerProtocol {
	proxy := &ServerProtocolProxy{[16]byte{}, nil}
	serverProtocol := &BinaryServerProtocol{slock, &sync.Mutex{}, stream, nil, make([]*ServerProtocolProxy, 0), make([]*protocol.LockCommand, FREE_COMMAND_MAX_SIZE),
		0, NewLockCommandQueue(4, 64, FREE_COMMAND_QUEUE_INIT_SIZE), make([]byte, 1024), make([]byte, 64),
		0, 0, nil, nil, 0, false, false}
	proxy.serverProtocol = serverProtocol
	serverProtocol.InitLockCommand()
	serverProtocol.session = slock.addServerProtocol(serverProtocol)
	stream.protocol = serverProtocol
	_ = serverProtocol.AddProxy(proxy)
	return serverProtocol
}

func (self *BinaryServerProtocol) FindCallMethod(methodName string) (BinaryServerProtocolCallHandler, error) {
	if self.callMethods == nil {
		self.callMethods = make(map[string]BinaryServerProtocolCallHandler, 8)
		self.callMethods["LIST_LOCK"] = self.commandHandleListLockCommand
		self.callMethods["LIST_LOCKED"] = self.commandHandleListLockedCommand
		self.callMethods["LIST_WAIT"] = self.commandHandleListWaitCommand

		for name, handler := range self.slock.GetReplicationManager().GetCallMethods() {
			self.callMethods[name] = handler
		}

		if self.slock.arbiterManager != nil {
			for name, handler := range self.slock.GetArbiterManager().GetCallMethods() {
				self.callMethods[name] = handler
			}
		}
	}
	if callMethod, ok := self.callMethods[methodName]; ok {
		return callMethod, nil
	}
	return nil, errors.New("unknown method")
}

func (self *BinaryServerProtocol) Init(clientId [16]byte) error {
	if self.inited {
		self.slock.clientsGlock.Lock()
		if sp, ok := self.slock.clients[self.proxys[0].clientId]; ok {
			if sp == self {
				delete(self.slock.clients, self.proxys[0].clientId)
			}
		}
		self.slock.clientsGlock.Unlock()
	}

	self.proxys[0].clientId = clientId
	self.inited = true
	return nil
}

func (self *BinaryServerProtocol) Close() error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return nil
	}

	self.closed = true
	for _, proxy := range self.proxys {
		proxy.serverProtocol = defaultServerProtocol
	}
	self.proxys = self.proxys[:1]
	_ = self.slock.removeServerProtocol(self.session)
	willCommands := self.willCommands
	if willCommands != nil {
		self.willCommands = nil
		self.glock.Unlock()

		for {
			command := willCommands.Pop()
			if command == nil {
				break
			}
			_ = self.ProcessCommad(command)
		}
	} else {
		self.glock.Unlock()
	}

	self.slock.clientsGlock.Lock()
	if self.inited {
		self.inited = false
		if sp, ok := self.slock.clients[self.proxys[0].clientId]; ok {
			if sp == self {
				delete(self.slock.clients, self.proxys[0].clientId)
			}
		}
	}
	self.slock.statsTotalCommandCount += self.totalCommandCount
	self.slock.clientsGlock.Unlock()

	self.glock.Lock()
	if self.stream != nil {
		err := self.stream.Close()
		if err != nil {
			self.slock.Log().Errorf("Protocol binary connection close error %s %v", self.RemoteAddr().String(), err)
		}
		self.stream.protocol = nil
	}

	self.UnInitLockCommand()
	self.session = nil
	self.glock.Unlock()
	return nil
}

func (self *BinaryServerProtocol) Lock() {
	self.glock.Lock()
}

func (self *BinaryServerProtocol) Unlock() {
	self.glock.Unlock()
}

func (self *BinaryServerProtocol) Read() (protocol.CommandDecode, error) {
	if self.closed {
		return nil, errors.New("Protocol Closed")
	}

	for self.rlen-self.rindex < 64 {
		n, err := self.stream.conn.Read(self.rbuf[self.rlen:])
		if err != nil {
			return nil, err
		}
		self.rlen += n
	}

	command, err := self.ReadParse(self.rbuf[self.rindex:])
	self.rindex += 64
	if self.rindex == self.rlen {
		self.rindex, self.rlen = 0, 0
	}
	return command, err
}

func (self *BinaryServerProtocol) ReadParse(buf []byte) (protocol.CommandDecode, error) {
	if len(buf) < 64 {
		return nil, errors.New("command data too short")
	}

	mv := uint16(buf[0]) | uint16(buf[1])<<8
	if mv != 0x0156 {
		if mv&0xff != uint16(protocol.MAGIC) {
			return nil, errors.New("unknown magic")
		}

		if (mv>>8)&0xff != uint16(protocol.VERSION) {
			return nil, errors.New("unknown version")
		}
	}

	commandType := uint8(buf[2])
	switch commandType {
	case protocol.COMMAND_LOCK:
		lockCommand := self.GetLockCommand()
		err := lockCommand.Decode(buf)
		if err != nil {
			return nil, err
		}
		return lockCommand, nil

	case protocol.COMMAND_UNLOCK:
		lockCommand := self.GetLockCommand()
		err := lockCommand.Decode(buf)
		if err != nil {
			return nil, err
		}
		return lockCommand, nil
	default:
		switch commandType {
		case protocol.COMMAND_INIT:
			initCommand := &protocol.InitCommand{}
			err := initCommand.Decode(buf)
			if err != nil {
				return nil, err
			}
			return initCommand, nil

		case protocol.CommandState:
			stateCommand := &protocol.StateCommand{}
			err := stateCommand.Decode(buf)
			if err != nil {
				return nil, err
			}
			return stateCommand, nil

		case protocol.COMMAND_ADMIN:
			adminCommand := &protocol.AdminCommand{}
			err := adminCommand.Decode(buf)
			if err != nil {
				return nil, err
			}
			return adminCommand, nil
		case protocol.COMMAND_PING:
			pingCommand := &protocol.PingCommand{}
			err := pingCommand.Decode(buf)
			if err != nil {
				return nil, err
			}
			return pingCommand, nil
		case protocol.COMMAND_QUIT:
			quitCommand := &protocol.QuitCommand{}
			err := quitCommand.Decode(buf)
			if err != nil {
				return nil, err
			}
			return quitCommand, nil
		case protocol.COMMAND_CALL:
			callCommand := &protocol.CallCommand{}
			err := callCommand.Decode(buf)
			if err != nil {
				return nil, err
			}
			callCommand.Data = make([]byte, callCommand.ContentLen)
			if callCommand.ContentLen > 0 {
				rindex, contentLen := self.rindex+64, int(callCommand.ContentLen)
				if self.rlen-rindex >= contentLen {
					copy(callCommand.Data, self.rbuf[rindex:rindex+contentLen])
					self.rindex += contentLen
					contentLen = 0
				} else if self.rlen-rindex > 0 {
					copy(callCommand.Data, self.rbuf[rindex:self.rlen])
					contentLen -= self.rlen - rindex
					self.rindex += self.rlen - rindex
				}

				if contentLen > 0 {
					_, err := self.stream.ReadBytes(callCommand.Data[int(callCommand.ContentLen)-contentLen:])
					if err != nil {
						return nil, err
					}
				}
			}
			return callCommand, nil
		case protocol.COMMAND_WILL_LOCK:
			lockCommand := self.GetLockCommand()
			err := lockCommand.Decode(buf)
			if err != nil {
				return nil, err
			}
			return lockCommand, nil
		case protocol.COMMAND_WILL_UNLOCK:
			lockCommand := self.GetLockCommand()
			err := lockCommand.Decode(buf)
			if err != nil {
				return nil, err
			}
			return lockCommand, nil
		case protocol.COMMAND_LEADER:
			leaderCommand := &protocol.LeaderCommand{}
			err := leaderCommand.Decode(buf)
			if err != nil {
				return nil, err
			}
			return leaderCommand, nil
		case protocol.COMMAND_SUBSCRIBE:
			leaderCommand := &protocol.SubscribeCommand{}
			err := leaderCommand.Decode(buf)
			if err != nil {
				return nil, err
			}
			return leaderCommand, nil
		}
	}
	return nil, errors.New("Unknown Command")
}

func (self *BinaryServerProtocol) Write(result protocol.CommandEncode) error {
	if self.closed {
		return errors.New("Protocol Closed")
	}

	self.glock.Lock()
	err := result.Encode(self.wbuf)
	if err != nil {
		self.glock.Unlock()
		return err
	}

	n, err := self.stream.conn.Write(self.wbuf)
	if err != nil {
		self.glock.Unlock()
		return err
	}

	if n < 64 {
		for n < 64 {
			cn, err := self.stream.conn.Write(self.wbuf[n:])
			if err != nil {
				self.glock.Unlock()
				return err
			}
			n += cn
		}
	}

	switch result.(type) {
	case *protocol.CallResultCommand:
		callCommand := result.(*protocol.CallResultCommand)
		if callCommand.ContentLen > 0 {
			err := self.stream.WriteBytes(callCommand.Data)
			if err != nil {
				self.glock.Unlock()
				return err
			}
		}
	}
	self.glock.Unlock()
	return nil
}

func (self *BinaryServerProtocol) ReadCommand() (protocol.CommandDecode, error) {
	return self.Read()
}

func (self *BinaryServerProtocol) WriteCommand(result protocol.CommandEncode) error {
	return self.Write(result)
}

func (self *BinaryServerProtocol) Process() error {
	buf := self.rbuf
	for !self.closed {
		for self.rlen-self.rindex < 64 {
			n, err := self.stream.conn.Read(buf[self.rlen:])
			if err != nil {
				return err
			}
			self.rlen += n
		}

		if self.slock.state != STATE_LEADER {
			return AGAIN
		}

		for self.rlen-self.rindex >= 64 {
			err := self.ProcessParse(buf[self.rindex:])
			self.rindex += 64
			if self.rindex == self.rlen {
				self.rindex, self.rlen = 0, 0
			}
			if err != nil {
				return err
			}
		}
	}
	return io.EOF
}

func (self *BinaryServerProtocol) ProcessParse(buf []byte) error {
	if len(buf) < 64 {
		return errors.New("command data too short")
	}

	mv := uint16(buf[0]) | uint16(buf[1])<<8
	if mv != 0x0156 {
		if mv&0xff != uint16(protocol.MAGIC) {
			command := protocol.Command{}
			err := command.Decode(buf)
			if err != nil {
				return err
			}
			_ = self.Write(protocol.NewResultCommand(&command, protocol.RESULT_UNKNOWN_MAGIC))
			return errors.New("Unknown Magic")
		}

		if (mv>>8)&0xff != uint16(protocol.VERSION) {
			command := protocol.Command{}
			err := command.Decode(buf)
			if err != nil {
				return err
			}
			_ = self.Write(protocol.NewResultCommand(&command, protocol.RESULT_UNKNOWN_VERSION))
			return errors.New("Unknown Version")
		}
	}

	self.totalCommandCount++
	var lockCommand *protocol.LockCommand
	commandType := uint8(buf[2])
	switch commandType {
	case protocol.COMMAND_LOCK:
		if self.freeCommandIndex > 0 {
			self.freeCommandIndex--
			lockCommand = self.freeCommands[self.freeCommandIndex]
		} else {
			lockCommand = self.GetLockCommandLocked()
		}

		lockCommand.CommandType = commandType

		lockCommand.RequestId[0], lockCommand.RequestId[1], lockCommand.RequestId[2], lockCommand.RequestId[3], lockCommand.RequestId[4], lockCommand.RequestId[5], lockCommand.RequestId[6], lockCommand.RequestId[7],
			lockCommand.RequestId[8], lockCommand.RequestId[9], lockCommand.RequestId[10], lockCommand.RequestId[11], lockCommand.RequestId[12], lockCommand.RequestId[13], lockCommand.RequestId[14], lockCommand.RequestId[15] =
			buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

		lockCommand.Flag, lockCommand.DbId = uint8(buf[19]), uint8(buf[20])

		lockCommand.LockId[0], lockCommand.LockId[1], lockCommand.LockId[2], lockCommand.LockId[3], lockCommand.LockId[4], lockCommand.LockId[5], lockCommand.LockId[6], lockCommand.LockId[7],
			lockCommand.LockId[8], lockCommand.LockId[9], lockCommand.LockId[10], lockCommand.LockId[11], lockCommand.LockId[12], lockCommand.LockId[13], lockCommand.LockId[14], lockCommand.LockId[15] =
			buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
			buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36]

		lockCommand.LockKey[0], lockCommand.LockKey[1], lockCommand.LockKey[2], lockCommand.LockKey[3], lockCommand.LockKey[4], lockCommand.LockKey[5], lockCommand.LockKey[6], lockCommand.LockKey[7],
			lockCommand.LockKey[8], lockCommand.LockKey[9], lockCommand.LockKey[10], lockCommand.LockKey[11], lockCommand.LockKey[12], lockCommand.LockKey[13], lockCommand.LockKey[14], lockCommand.LockKey[15] =
			buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44],
			buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52]

		lockCommand.Timeout, lockCommand.TimeoutFlag, lockCommand.Expried, lockCommand.ExpriedFlag = uint16(buf[53])|uint16(buf[54])<<8, uint16(buf[55])|uint16(buf[56])<<8, uint16(buf[57])|uint16(buf[58])<<8, uint16(buf[59])|uint16(buf[60])<<8
		lockCommand.Count, lockCommand.Rcount = uint16(buf[61])|uint16(buf[62])<<8, uint8(buf[63])

		if lockCommand.DbId == 0xff {
			err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.FreeLockCommand(lockCommand)
			return err
		}

		db := self.slock.dbs[lockCommand.DbId]
		if db == nil {
			db = self.slock.GetOrNewDB(lockCommand.DbId)
		}
		err := db.Lock(self, lockCommand)
		if err != nil {
			return err
		}
		return nil
	case protocol.COMMAND_UNLOCK:
		if self.freeCommandIndex > 0 {
			self.freeCommandIndex--
			lockCommand = self.freeCommands[self.freeCommandIndex]
		} else {
			lockCommand = self.GetLockCommandLocked()
		}

		lockCommand.CommandType = commandType

		lockCommand.RequestId[0], lockCommand.RequestId[1], lockCommand.RequestId[2], lockCommand.RequestId[3], lockCommand.RequestId[4], lockCommand.RequestId[5], lockCommand.RequestId[6], lockCommand.RequestId[7],
			lockCommand.RequestId[8], lockCommand.RequestId[9], lockCommand.RequestId[10], lockCommand.RequestId[11], lockCommand.RequestId[12], lockCommand.RequestId[13], lockCommand.RequestId[14], lockCommand.RequestId[15] =
			buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

		lockCommand.Flag, lockCommand.DbId = uint8(buf[19]), uint8(buf[20])

		lockCommand.LockId[0], lockCommand.LockId[1], lockCommand.LockId[2], lockCommand.LockId[3], lockCommand.LockId[4], lockCommand.LockId[5], lockCommand.LockId[6], lockCommand.LockId[7],
			lockCommand.LockId[8], lockCommand.LockId[9], lockCommand.LockId[10], lockCommand.LockId[11], lockCommand.LockId[12], lockCommand.LockId[13], lockCommand.LockId[14], lockCommand.LockId[15] =
			buf[21], buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28],
			buf[29], buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36]

		lockCommand.LockKey[0], lockCommand.LockKey[1], lockCommand.LockKey[2], lockCommand.LockKey[3], lockCommand.LockKey[4], lockCommand.LockKey[5], lockCommand.LockKey[6], lockCommand.LockKey[7],
			lockCommand.LockKey[8], lockCommand.LockKey[9], lockCommand.LockKey[10], lockCommand.LockKey[11], lockCommand.LockKey[12], lockCommand.LockKey[13], lockCommand.LockKey[14], lockCommand.LockKey[15] =
			buf[37], buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44],
			buf[45], buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52]

		lockCommand.Timeout, lockCommand.TimeoutFlag, lockCommand.Expried, lockCommand.ExpriedFlag = uint16(buf[53])|uint16(buf[54])<<8, uint16(buf[55])|uint16(buf[56])<<8, uint16(buf[57])|uint16(buf[58])<<8, uint16(buf[59])|uint16(buf[60])<<8
		lockCommand.Count, lockCommand.Rcount = uint16(buf[61])|uint16(buf[62])<<8, uint8(buf[63])

		if lockCommand.DbId == 0xff {
			err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.FreeLockCommand(lockCommand)
			return err
		}

		db := self.slock.dbs[lockCommand.DbId]
		if db == nil {
			err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.FreeLockCommand(lockCommand)
			return err
		}
		err := db.UnLock(self, lockCommand)
		if err != nil {
			return err
		}
		return nil
	default:
		var command protocol.ICommand
		switch commandType {
		case protocol.COMMAND_INIT:
			command = &protocol.InitCommand{}
		case protocol.CommandState:
			command = &protocol.StateCommand{}
		case protocol.COMMAND_ADMIN:
			command = &protocol.AdminCommand{}
		case protocol.COMMAND_PING:
			command = &protocol.PingCommand{}
		case protocol.COMMAND_QUIT:
			command = &protocol.QuitCommand{}
		case protocol.COMMAND_CALL:
			callCommand := protocol.CallCommand{}
			err := callCommand.Decode(buf)
			if err != nil {
				return err
			}

			callCommand.Data = make([]byte, callCommand.ContentLen)
			if callCommand.ContentLen > 0 {
				rindex, contentLen := self.rindex+64, int(callCommand.ContentLen)
				if self.rlen-rindex >= contentLen {
					copy(callCommand.Data, self.rbuf[rindex:rindex+contentLen])
					self.rindex += contentLen
					contentLen = 0
				} else if self.rlen-rindex > 0 {
					copy(callCommand.Data, self.rbuf[rindex:self.rlen])
					contentLen -= self.rlen - rindex
					self.rindex += self.rlen - rindex
				}

				if contentLen > 0 {
					_, err := self.stream.ReadBytes(callCommand.Data[int(callCommand.ContentLen)-contentLen:])
					if err != nil {
						return err
					}
				}
			}
			err = self.ProcessCommad(&callCommand)
			if err != nil {
				return err
			}
			return nil
		case protocol.COMMAND_WILL_LOCK:
			command = self.GetLockCommand()
		case protocol.COMMAND_WILL_UNLOCK:
			command = self.GetLockCommand()
		case protocol.COMMAND_LEADER:
			command = &protocol.LeaderCommand{}
		case protocol.COMMAND_SUBSCRIBE:
			command = &protocol.SubscribeCommand{}
		default:
			command = &protocol.Command{}
		}
		err := command.Decode(buf)
		if err != nil {
			return err
		}
		err = self.ProcessCommad(command)
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *BinaryServerProtocol) ProcessBuild(command protocol.ICommand) error {
	return self.Write(command)
}

func (self *BinaryServerProtocol) ProcessCommad(command protocol.ICommand) error {
	switch command.GetCommandType() {
	case protocol.COMMAND_LOCK:
		lockCommand := command.(*protocol.LockCommand)

		if lockCommand.DbId == 0xff {
			err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.FreeLockCommand(lockCommand)
			return err
		}

		db := self.slock.dbs[lockCommand.DbId]
		if db == nil {
			db = self.slock.GetOrNewDB(lockCommand.DbId)
		}
		return db.Lock(self, lockCommand)

	case protocol.COMMAND_UNLOCK:
		lockCommand := command.(*protocol.LockCommand)

		if lockCommand.DbId == 0xff {
			err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.FreeLockCommand(lockCommand)
			return err
		}

		db := self.slock.dbs[lockCommand.DbId]
		if db == nil {
			err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.FreeLockCommand(lockCommand)
			return err
		}
		return db.UnLock(self, lockCommand)

	default:
		switch command.GetCommandType() {
		case protocol.COMMAND_INIT:
			initCommand := command.(*protocol.InitCommand)
			err := self.Init(initCommand.ClientId)
			if err != nil {
				return self.Write(protocol.NewInitResultCommand(initCommand, protocol.RESULT_ERROR, 0))
			}

			self.slock.clientsGlock.Lock()
			initType := uint8(0)
			if _, ok := self.slock.clients[initCommand.ClientId]; ok {
				initType = 1
			}
			self.slock.clients[initCommand.ClientId] = self
			self.slock.clientsGlock.Unlock()
			return self.Write(protocol.NewInitResultCommand(initCommand, protocol.RESULT_SUCCEED, initType))

		case protocol.CommandState:
			return self.slock.GetState(self, command.(*protocol.StateCommand))

		case protocol.COMMAND_ADMIN:
			adminCommand := command.(*protocol.AdminCommand)
			err := self.Write(protocol.NewAdminResultCommand(adminCommand, protocol.RESULT_SUCCEED))
			if err != nil {
				return err
			}

			serverProtocol := NewTextServerProtocol(self.slock, self.stream)
			err = serverProtocol.Process()
			if err != nil {
				if err != io.EOF {
					self.slock.Log().Errorf("Protocol binary connection process error %s %v", self.RemoteAddr().String(), err)
				}
			}

			if self.stream != nil {
				self.stream.protocol = self
			}
			self.totalCommandCount += serverProtocol.totalCommandCount
			serverProtocol.UnInitLockCommand()
			serverProtocol.closed = true
			return err

		case protocol.COMMAND_PING:
			pingCommand := command.(*protocol.PingCommand)
			return self.Write(protocol.NewPingResultCommand(pingCommand, protocol.RESULT_SUCCEED))

		case protocol.COMMAND_QUIT:
			quitCommand := command.(*protocol.QuitCommand)
			err := self.Write(protocol.NewQuitResultCommand(quitCommand, protocol.RESULT_SUCCEED))
			if err == nil {
				return io.EOF
			}
			return err

		case protocol.COMMAND_CALL:
			callCommand := command.(*protocol.CallCommand)
			if handler, err := self.FindCallMethod(callCommand.MethodName); err == nil {
				resultCommand, rerr := handler(self, callCommand)
				if resultCommand == nil {
					return rerr
				}

				err := self.Write(resultCommand)
				if err == nil {
					return rerr
				}
				return err
			}
			return self.Write(protocol.NewCallResultCommand(callCommand, protocol.RESULT_UNKNOWN_COMMAND, "", nil))

		case protocol.COMMAND_WILL_LOCK:
			if self.willCommands == nil {
				self.glock.Lock()
				if self.willCommands == nil {
					self.willCommands = NewLockCommandQueue(2, 4, 8)
				}
				self.glock.Unlock()
			}
			lockCommand := command.(*protocol.LockCommand)
			lockCommand.CommandType = protocol.COMMAND_LOCK
			return self.willCommands.Push(lockCommand)

		case protocol.COMMAND_WILL_UNLOCK:
			if self.willCommands == nil {
				self.glock.Lock()
				if self.willCommands == nil {
					self.willCommands = NewLockCommandQueue(2, 4, 8)
				}
				self.glock.Unlock()
			}
			lockCommand := command.(*protocol.LockCommand)
			lockCommand.CommandType = protocol.COMMAND_UNLOCK
			return self.willCommands.Push(lockCommand)
		case protocol.COMMAND_LEADER:
			leaderCommand := command.(*protocol.LeaderCommand)
			return self.Write(protocol.NewLeaderResultCommand(leaderCommand, protocol.RESULT_SUCCEED, self.slock.replicationManager.transparencyManager.leaderAddress))
		case protocol.COMMAND_SUBSCRIBE:
			subscribeCommand := command.(*protocol.SubscribeCommand)
			subscribeResultCommand, err := self.slock.subscribeManager.handleSubscribeCommand(self, subscribeCommand)
			if err != nil {
				return err
			}
			return self.Write(subscribeResultCommand)
		default:
			return self.Write(protocol.NewResultCommand(command, protocol.RESULT_UNKNOWN_COMMAND))
		}
	}
}

func (self *BinaryServerProtocol) ProcessLockCommand(lockCommand *protocol.LockCommand) error {
	if lockCommand.DbId == 0xff {
		err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
		_ = self.FreeLockCommand(lockCommand)
		return err
	}

	db := self.slock.dbs[lockCommand.DbId]
	if lockCommand.CommandType == protocol.COMMAND_LOCK {
		if db == nil {
			db = self.slock.GetOrNewDB(lockCommand.DbId)
		}
		return db.Lock(self, lockCommand)
	}

	if db == nil {
		err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
		_ = self.FreeLockCommand(lockCommand)
		return err
	}
	return db.UnLock(self, lockCommand)
}

func (self *BinaryServerProtocol) ProcessLockResultCommand(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	if self.closed {
		if !self.inited {
			return errors.New("Protocol Closed")
		}

		self.slock.clientsGlock.Lock()
		if serverProtocol, ok := self.slock.clients[self.proxys[0].clientId]; ok {
			self.slock.clientsGlock.Unlock()
			return serverProtocol.ProcessLockResultCommandLocked(command, result, lcount, lrcount)
		}
		self.slock.clientsGlock.Unlock()
		return errors.New("Protocol Closed")
	}

	buf := self.wbuf
	if len(buf) < 64 {
		return errors.New("buf too short")
	}

	self.glock.Lock()
	buf[0], buf[1], buf[2] = protocol.MAGIC, protocol.VERSION, byte(command.CommandType)

	buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18] =
		command.RequestId[0], command.RequestId[1], command.RequestId[2], command.RequestId[3], command.RequestId[4], command.RequestId[5], command.RequestId[6], command.RequestId[7],
		command.RequestId[8], command.RequestId[9], command.RequestId[10], command.RequestId[11], command.RequestId[12], command.RequestId[13], command.RequestId[14], command.RequestId[15]

	buf[19], buf[20], buf[21] = result, 0x00, byte(command.DbId)

	buf[22], buf[23], buf[24], buf[25], buf[26], buf[27], buf[28], buf[29],
		buf[30], buf[31], buf[32], buf[33], buf[34], buf[35], buf[36], buf[37] =
		command.LockId[0], command.LockId[1], command.LockId[2], command.LockId[3], command.LockId[4], command.LockId[5], command.LockId[6], command.LockId[7],
		command.LockId[8], command.LockId[9], command.LockId[10], command.LockId[11], command.LockId[12], command.LockId[13], command.LockId[14], command.LockId[15]

	buf[38], buf[39], buf[40], buf[41], buf[42], buf[43], buf[44], buf[45],
		buf[46], buf[47], buf[48], buf[49], buf[50], buf[51], buf[52], buf[53] =
		command.LockKey[0], command.LockKey[1], command.LockKey[2], command.LockKey[3], command.LockKey[4], command.LockKey[5], command.LockKey[6], command.LockKey[7],
		command.LockKey[8], command.LockKey[9], command.LockKey[10], command.LockKey[11], command.LockKey[12], command.LockKey[13], command.LockKey[14], command.LockKey[15]

	buf[54], buf[55], buf[56], buf[57], buf[58], buf[59], buf[60], buf[61] = byte(lcount), byte(lcount>>8), byte(command.Count), byte(command.Count>>8), byte(lrcount), byte(command.Rcount), 0x00, 0x00
	buf[62], buf[63] = 0x00, 0x00

	n, err := self.stream.conn.Write(buf)
	if err != nil {
		self.glock.Unlock()
		return err
	}

	if n < 64 {
		for n < 64 {
			cn, err := self.stream.conn.Write(buf[n:])
			if err != nil {
				self.glock.Unlock()
				return err
			}
			n += cn
		}
	}
	self.glock.Unlock()
	return nil
}

func (self *BinaryServerProtocol) ProcessLockResultCommandLocked(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	return self.ProcessLockResultCommand(command, result, lcount, lrcount)
}

func (self *BinaryServerProtocol) GetStream() *Stream {
	return self.stream
}

func (self *BinaryServerProtocol) GetProxy() *ServerProtocolProxy {
	return self.proxys[0]
}

func (self *BinaryServerProtocol) AddProxy(proxy *ServerProtocolProxy) error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return errors.New("closed")
	}

	self.proxys = append(self.proxys, proxy)
	self.glock.Unlock()
	return nil
}

func (self *BinaryServerProtocol) RemoteAddr() net.Addr {
	if self.stream == nil {
		return &net.TCPAddr{IP: []byte("0.0.0.0"), Port: 0, Zone: ""}
	}
	return self.stream.RemoteAddr()
}

func (self *BinaryServerProtocol) InitLockCommand() {
	self.slock.freeLockCommandLock.Lock()
	for i := 0; i < 4; i++ {
		lockCommand := self.slock.freeLockCommandQueue.PopRight()
		if lockCommand == nil {
			break
		}
		self.slock.freeLockCommandCount--
		self.freeCommands[self.freeCommandIndex] = lockCommand
		self.freeCommandIndex++
	}
	self.slock.freeLockCommandLock.Unlock()
}

func (self *BinaryServerProtocol) UnInitLockCommand() {
	self.slock.freeLockCommandLock.Lock()
	for self.freeCommandIndex > 0 {
		self.freeCommandIndex--
		command := self.freeCommands[self.freeCommandIndex]
		_ = self.slock.freeLockCommandQueue.Push(command)
		self.slock.freeLockCommandCount++
	}

	for {
		command := self.lockedFreeCommands.PopRight()
		if command == nil {
			break
		}
		_ = self.slock.freeLockCommandQueue.Push(command)
		self.slock.freeLockCommandCount++
	}
	self.slock.freeLockCommandLock.Unlock()
}

func (self *BinaryServerProtocol) GetLockCommand() *protocol.LockCommand {
	if self.freeCommandIndex > 0 {
		self.freeCommandIndex--
		return self.freeCommands[self.freeCommandIndex]
	}
	return self.GetLockCommandLocked()
}

func (self *BinaryServerProtocol) GetLockCommandLocked() *protocol.LockCommand {
	self.glock.Lock()
	lockCommand := self.lockedFreeCommands.PopRight()
	if lockCommand != nil {
		self.glock.Unlock()
		return lockCommand
	}
	self.glock.Unlock()

	self.slock.freeLockCommandLock.Lock()
	lockCommand = self.slock.freeLockCommandQueue.PopRight()
	if lockCommand != nil {
		self.slock.freeLockCommandCount--
		self.slock.freeLockCommandLock.Unlock()
		return lockCommand
	}
	self.slock.freeLockCommandLock.Unlock()
	return &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION}}
}

func (self *BinaryServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
	if self.freeCommandIndex < FREE_COMMAND_MAX_SIZE {
		self.freeCommands[self.freeCommandIndex] = command
		self.freeCommandIndex++
		return nil
	}
	return self.FreeLockCommandLocked(command)
}

func (self *BinaryServerProtocol) FreeLockCommandLocked(command *protocol.LockCommand) error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		self.slock.freeLockCommandLock.Lock()
		_ = self.slock.freeLockCommandQueue.Push(command)
		self.slock.freeLockCommandCount++
		self.slock.freeLockCommandLock.Unlock()
		return nil
	}
	_ = self.lockedFreeCommands.Push(command)
	self.glock.Unlock()
	return nil
}

func (self *BinaryServerProtocol) commandHandleListLockCommand(_ *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.slock.state != STATE_LEADER {
		return protocol.NewCallResultCommand(command, protocol.RESULT_STATE_ERROR, "STATE_ERROR", nil), nil
	}

	request := protobuf.LockDBListLockRequest{}
	err := proto.Unmarshal(command.Data, &request)
	if err != nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_ERROR, "DECODE_ERROR", nil), nil
	}

	db := self.slock.dbs[request.DbId]
	if db == nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_UNKNOWN_DB, "UNKNOWN_DB_ERROR", nil), nil
	}

	locks := make([]*protobuf.LockDBLock, 0)
	for _, value := range db.fastLocks {
		lockManager := value.manager
		if lockManager != nil && lockManager.locked > 0 {
			locks = append(locks, &protobuf.LockDBLock{LockKey: lockManager.lockKey[:], LockedCount: lockManager.locked})
		}
	}

	db.mGlock.Lock()
	for _, lockManager := range db.locks {
		if lockManager.locked > 0 {
			locks = append(locks, &protobuf.LockDBLock{LockKey: lockManager.lockKey[:], LockedCount: lockManager.locked})
		}
	}
	db.mGlock.Unlock()

	response := protobuf.LockDBListLockResponse{Locks: locks}
	data, err := proto.Marshal(&response)
	if err != nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_ERROR, "ENCODE_ERROR", nil), nil
	}
	return protocol.NewCallResultCommand(command, protocol.RESULT_SUCCEED, "", data), nil
}

func (self *BinaryServerProtocol) commandHandleListLockedCommand(_ *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.slock.state != STATE_LEADER {
		return protocol.NewCallResultCommand(command, protocol.RESULT_STATE_ERROR, "STATE_ERROR", nil), nil
	}

	request := protobuf.LockDBListLockedRequest{}
	err := proto.Unmarshal(command.Data, &request)
	if err != nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_ERROR, "DECODE_ERROR", nil), nil
	}

	db := self.slock.dbs[request.DbId]
	if db == nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_UNKNOWN_DB, "UNKNOWN_DB_ERROR", nil), nil
	}

	lockKey := [16]byte{}
	copy(lockKey[:], request.LockKey)
	lockCommand := protocol.LockCommand{LockKey: lockKey}
	lockManager := db.GetLockManager(&lockCommand)
	if lockManager == nil || lockManager.locked <= 0 {
		return protocol.NewCallResultCommand(command, protocol.RESULT_UNKNOWN_DB, "LOCK_MANAGER_ERROR", nil), nil
	}

	locks := make([]*protobuf.LockDBLockLocked, 0)
	lockManager.glock.Lock()
	if lockManager.currentLock != nil {
		lock := lockManager.currentLock

		lockedCommand := &protobuf.LockDBLockCommand{RequestId: lock.command.RequestId[:], Flag: uint32(lock.command.Flag), LockId: lock.command.LockId[:],
			LockKey: lock.command.LockKey[:], TimeoutFlag: uint32(lock.command.TimeoutFlag), Timeout: uint32(lock.command.Timeout), ExpriedFlag: uint32(lock.command.ExpriedFlag),
			Expried: uint32(lock.command.Expried), Count: uint32(lock.command.Count), Rcount: uint32(lock.command.Rcount)}
		lockedLock := &protobuf.LockDBLockLocked{LockId: lock.command.LockId[:], StartTime: uint64(lock.startTime), TimeoutTime: uint64(lock.timeoutTime),
			ExpriedTime: uint64(lock.expriedTime), LockedCount: uint32(lock.locked), AofTime: uint32(lock.aofTime), IsTimeouted: lock.timeouted, IsExpried: lock.expried,
			IsAof: lock.isAof, IsLongTime: lock.longWaitIndex > 0, Command: lockedCommand}
		locks = append(locks, lockedLock)

	}

	if lockManager.locks != nil {
		for i := range lockManager.locks.IterNodes() {
			nodeQueues := lockManager.locks.IterNodeQueues(int32(i))
			for _, lock := range nodeQueues {
				if lock.locked == 0 {
					continue
				}

				lockedCommand := &protobuf.LockDBLockCommand{RequestId: lock.command.RequestId[:], Flag: uint32(lock.command.Flag), LockId: lock.command.LockId[:],
					LockKey: lock.command.LockKey[:], TimeoutFlag: uint32(lock.command.TimeoutFlag), Timeout: uint32(lock.command.Timeout), ExpriedFlag: uint32(lock.command.ExpriedFlag),
					Expried: uint32(lock.command.Expried), Count: uint32(lock.command.Count), Rcount: uint32(lock.command.Rcount)}
				lockedLock := &protobuf.LockDBLockLocked{LockId: lock.command.LockId[:], StartTime: uint64(lock.startTime), TimeoutTime: uint64(lock.timeoutTime),
					ExpriedTime: uint64(lock.expriedTime), LockedCount: uint32(lock.locked), AofTime: uint32(lock.aofTime), IsTimeouted: lock.timeouted, IsExpried: lock.expried,
					IsAof: lock.isAof, IsLongTime: lock.longWaitIndex > 0, Command: lockedCommand}
				locks = append(locks, lockedLock)
			}
		}
	}
	lockManager.glock.Unlock()

	response := protobuf.LockDBListLockedResponse{LockKey: lockManager.lockKey[:], LockedCount: lockManager.locked, Locks: locks}
	data, err := proto.Marshal(&response)
	if err != nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_ERROR, "ENCODE_ERROR", nil), nil
	}
	return protocol.NewCallResultCommand(command, protocol.RESULT_SUCCEED, "", data), nil
}

func (self *BinaryServerProtocol) commandHandleListWaitCommand(_ *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.slock.state != STATE_LEADER {
		return protocol.NewCallResultCommand(command, protocol.RESULT_STATE_ERROR, "STATE_ERROR", nil), nil
	}

	request := protobuf.LockDBListWaitRequest{}
	err := proto.Unmarshal(command.Data, &request)
	if err != nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_ERROR, "DECODE_ERROR", nil), nil
	}

	db := self.slock.dbs[request.DbId]
	if db == nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_UNKNOWN_DB, "UNKNOWN_DB_ERROR", nil), nil
	}

	lockKey := [16]byte{}
	copy(lockKey[:], request.LockKey)
	lockCommand := protocol.LockCommand{LockKey: lockKey}
	lockManager := db.GetLockManager(&lockCommand)
	if lockManager == nil || lockManager.locked <= 0 {
		return protocol.NewCallResultCommand(command, protocol.RESULT_UNKNOWN_DB, "LOCK_MANAGER_ERROR", nil), nil
	}

	locks := make([]*protobuf.LockDBLockWait, 0)
	lockManager.glock.Lock()
	if lockManager.waitLocks != nil {
		for i := range lockManager.waitLocks.IterNodes() {
			nodeQueues := lockManager.waitLocks.IterNodeQueues(int32(i))
			for _, lock := range nodeQueues {
				if lock.timeouted {
					continue
				}

				waitCommand := &protobuf.LockDBLockCommand{RequestId: lock.command.RequestId[:], Flag: uint32(lock.command.Flag), LockId: lock.command.LockId[:],
					LockKey: lock.command.LockKey[:], TimeoutFlag: uint32(lock.command.TimeoutFlag), Timeout: uint32(lock.command.Timeout), ExpriedFlag: uint32(lock.command.ExpriedFlag),
					Expried: uint32(lock.command.Expried), Count: uint32(lock.command.Count), Rcount: uint32(lock.command.Rcount)}
				waitLock := &protobuf.LockDBLockWait{LockId: lock.command.LockId[:], StartTime: uint64(lock.startTime), TimeoutTime: uint64(lock.timeoutTime),
					IsLongTime: lock.longWaitIndex > 0, Command: waitCommand}
				locks = append(locks, waitLock)
			}
		}
	}
	lockManager.glock.Unlock()

	response := protobuf.LockDBListWaitResponse{LockKey: lockManager.lockKey[:], LockedCount: lockManager.locked, Locks: locks}
	data, err := proto.Marshal(&response)
	if err != nil {
		return protocol.NewCallResultCommand(command, protocol.RESULT_ERROR, "ENCODE_ERROR", nil), nil
	}
	return protocol.NewCallResultCommand(command, protocol.RESULT_SUCCEED, "", data), nil
}

type TextServerProtocolCommandHandler func(*TextServerProtocol, []string) error

type TextServerProtocol struct {
	slock              *SLock
	glock              *sync.Mutex
	stream             *Stream
	session            *ServerProtocolSession
	proxys             []*ServerProtocolProxy
	freeCommands       []*protocol.LockCommand
	freeCommandIndex   int
	lockedFreeCommands *LockCommandQueue
	freeCommandResult  *protocol.LockResultCommand
	parser             *protocol.TextParser
	handlers           map[string]TextServerProtocolCommandHandler
	lockWaiter         chan *protocol.LockResultCommand
	lockRequestId      [16]byte
	lockId             [16]byte
	willCommands       *LockCommandQueue
	totalCommandCount  uint64
	dbId               uint8
	closed             bool
}

func NewTextServerProtocol(slock *SLock, stream *Stream) *TextServerProtocol {
	proxy := &ServerProtocolProxy{[16]byte{}, nil}
	parser := protocol.NewTextParser(make([]byte, 1024), make([]byte, 1024))
	serverProtocol := &TextServerProtocol{slock, &sync.Mutex{}, stream, nil, make([]*ServerProtocolProxy, 0), make([]*protocol.LockCommand, FREE_COMMAND_MAX_SIZE),
		0, NewLockCommandQueue(4, 64, FREE_COMMAND_QUEUE_INIT_SIZE), nil, parser,
		nil, make(chan *protocol.LockResultCommand, 4), [16]byte{}, [16]byte{}, nil, 0, 0, false}
	proxy.serverProtocol = serverProtocol
	serverProtocol.InitLockCommand()
	serverProtocol.session = slock.addServerProtocol(serverProtocol)
	stream.protocol = serverProtocol
	_ = serverProtocol.AddProxy(proxy)
	return serverProtocol
}

func (self *TextServerProtocol) FindHandler(name string) (TextServerProtocolCommandHandler, error) {
	if self.handlers == nil {
		self.handlers = make(map[string]TextServerProtocolCommandHandler, 16)
		self.handlers["SELECT"] = self.commandHandlerSelectDB
		self.handlers["LOCK"] = self.commandHandlerLock
		self.handlers["UNLOCK"] = self.commandHandlerUnlock
		self.handlers["PUSH"] = self.commandHandlerPush
		for name, handler := range self.slock.GetAdmin().GetHandlers() {
			self.handlers[name] = handler
		}
	}

	if handler, ok := self.handlers[name]; ok {
		return handler, nil
	}
	return nil, errors.New("unknown command")
}

func (self *TextServerProtocol) Init(_ [16]byte) error {
	return nil
}

func (self *TextServerProtocol) Lock() {
	self.glock.Lock()
}

func (self *TextServerProtocol) Unlock() {
	self.glock.Unlock()
}

func (self *TextServerProtocol) Close() error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return nil
	}

	self.closed = true
	for _, proxy := range self.proxys {
		proxy.serverProtocol = defaultServerProtocol
	}
	self.proxys = self.proxys[:1]
	_ = self.slock.removeServerProtocol(self.session)
	willCommands := self.willCommands
	if self.willCommands != nil {
		self.willCommands = nil
		self.glock.Unlock()

		for {
			command := willCommands.Pop()
			if command == nil {
				break
			}
			_ = self.ProcessCommad(command)
		}
	} else {
		self.glock.Unlock()
	}

	self.slock.clientsGlock.Lock()
	self.slock.statsTotalCommandCount += self.totalCommandCount
	self.slock.clientsGlock.Unlock()

	self.glock.Lock()
	if self.stream != nil {
		err := self.stream.Close()
		if err != nil {
			self.slock.Log().Errorf("Protocol text connection close error %s %v", self.RemoteAddr().String(), err)
		}
		self.stream.protocol = nil
	}

	self.UnInitLockCommand()
	self.session = nil
	self.glock.Unlock()
	return nil
}

func (self *TextServerProtocol) GetParser() *protocol.TextParser {
	return self.parser
}

func (self *TextServerProtocol) Read() (protocol.CommandDecode, error) {
	rbuf := self.parser.GetReadBuf()
	for !self.closed {
		if self.parser.IsBufferEnd() {
			n, err := self.stream.Read(rbuf)
			if err != nil {
				return nil, err
			}

			self.parser.BufferUpdate(n)
		}

		err := self.parser.ParseRequest()
		if err != nil {
			return nil, err
		}

		if self.parser.IsParseFinish() {
			command, err := self.parser.GetRequestCommand()
			self.parser.Reset()
			return command, err
		}
	}
	return nil, errors.New("Protocol Closed")
}

func (self *TextServerProtocol) Write(result protocol.CommandEncode) error {
	switch result.(type) {
	case *protocol.LockResultCommand:
		return self.WriteCommand(result)
	case *protocol.TextResponseCommand:
		return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR Unknwon Command", nil))
	}
	return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR Unknwon Command", nil))
}

func (self *TextServerProtocol) ReadCommand() (protocol.CommandDecode, error) {
	command, err := self.Read()
	if err != nil {
		return nil, err
	}

	textServerCommand := command.(*protocol.TextRequestCommand)
	commandName := strings.ToUpper(textServerCommand.Args[0])
	if commandName == "LOCK" || commandName == "UNLOCK" {
		if len(textServerCommand.Args) < 5 {
			return nil, errors.New("Command Parse error")
		}

		command, err := self.ArgsToLockComand(textServerCommand.Args)
		return command, err
	}
	return nil, errors.New("unknown command")
}

func (self *TextServerProtocol) WriteCommand(result protocol.CommandEncode) error {
	if self.closed {
		return errors.New("Protocol Closed")
	}

	switch result.(type) {
	case *protocol.LockResultCommand:
		lockResultCommand := result.(*protocol.LockResultCommand)
		lockResults := []string{
			fmt.Sprintf("%d", lockResultCommand.Result),
			protocol.ERROR_MSG[lockResultCommand.Result],
			"LOCK_ID",
			fmt.Sprintf("%x", lockResultCommand.LockId),
			"LCOUNT",
			fmt.Sprintf("%d", lockResultCommand.Lcount),
			"COUNT",
			fmt.Sprintf("%d", lockResultCommand.Count),
			"LRCOUNT",
			fmt.Sprintf("%d", lockResultCommand.Lrcount),
			"RCOUNT",
			fmt.Sprintf("%d", lockResultCommand.Rcount),
		}
		return self.stream.WriteBytes(self.parser.BuildResponse(true, "", lockResults))
	}
	return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR Unknwon Command", nil))
}

func (self *TextServerProtocol) Process() error {
	rbuf := self.parser.GetReadBuf()
	for !self.closed {
		if self.parser.IsBufferEnd() {
			n, err := self.stream.Read(rbuf)
			if err != nil {
				return err
			}

			self.parser.BufferUpdate(n)
		}

		err := self.parser.ParseRequest()
		if err != nil {
			return err
		}

		if self.parser.IsParseFinish() {
			if self.slock.state != STATE_LEADER {
				return AGAIN
			}

			self.totalCommandCount++
			commandName := self.parser.GetCommandType()
			if commandHandler, err := self.FindHandler(commandName); err == nil {
				err := commandHandler(self, self.parser.GetArgs())
				if err != nil {
					return err
				}
			} else {
				err := self.commandHandlerUnknownCommand(self, self.parser.GetArgs())
				if err != nil {
					return err
				}
			}

			self.parser.Reset()
		}
	}
	return nil
}

func (self *TextServerProtocol) RunCommand() error {
	self.totalCommandCount++
	commandName := self.parser.GetCommandType()
	if commandHandler, err := self.FindHandler(commandName); err == nil {
		err := commandHandler(self, self.parser.GetArgs())
		if err != nil {
			return err
		}
	} else {
		err := self.commandHandlerUnknownCommand(self, self.parser.GetArgs())
		if err != nil {
			return err
		}
	}

	self.parser.Reset()
	return nil
}

func (self *TextServerProtocol) ProcessParse(buf []byte) error {
	self.parser.CopyToReadBuf(buf)
	err := self.parser.ParseRequest()
	if err != nil {
		return err
	}

	if self.parser.IsParseFinish() {
		self.totalCommandCount++
		commandName := self.parser.GetCommandType()
		if commandHandler, err := self.FindHandler(commandName); err == nil {
			err := commandHandler(self, self.parser.GetArgs())
			if err != nil {
				return err
			}
		} else {
			err := self.commandHandlerUnknownCommand(self, self.parser.GetArgs())
			if err != nil {
				return err
			}
		}

		self.parser.Reset()
	}
	return nil
}

func (self *TextServerProtocol) ProcessBuild(command protocol.ICommand) error {
	switch command.GetCommandType() {
	case protocol.COMMAND_LOCK:
		lockResultCommand := command.(*protocol.LockResultCommand)
		lockResults := []string{
			fmt.Sprintf("%d", lockResultCommand.Result),
			protocol.ERROR_MSG[lockResultCommand.Result],
			"LOCK_ID",
			fmt.Sprintf("%x", lockResultCommand.LockId),
			"LCOUNT",
			fmt.Sprintf("%d", lockResultCommand.Lcount),
			"COUNT",
			fmt.Sprintf("%d", lockResultCommand.Count),
			"LRCOUNT",
			fmt.Sprintf("%d", lockResultCommand.Lrcount),
			"RCOUNT",
			fmt.Sprintf("%d", lockResultCommand.Rcount),
		}
		return self.stream.WriteBytes(self.parser.BuildResponse(true, "", lockResults))
	case protocol.COMMAND_UNLOCK:
		lockResultCommand := command.(*protocol.LockResultCommand)
		lockResults := []string{
			fmt.Sprintf("%d", lockResultCommand.Result),
			protocol.ERROR_MSG[lockResultCommand.Result],
			"LOCK_ID",
			fmt.Sprintf("%x", lockResultCommand.LockId),
			"LCOUNT",
			fmt.Sprintf("%d", lockResultCommand.Lcount),
			"COUNT",
			fmt.Sprintf("%d", lockResultCommand.Count),
			"LRCOUNT",
			fmt.Sprintf("%d", lockResultCommand.Lrcount),
			"RCOUNT",
			fmt.Sprintf("%d", lockResultCommand.Rcount),
		}
		return self.stream.WriteBytes(self.parser.BuildResponse(true, "", lockResults))
	}
	return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR Unknwon Command", nil))
}

func (self *TextServerProtocol) ProcessCommad(command protocol.ICommand) error {
	switch command.GetCommandType() {
	case protocol.COMMAND_LOCK:
		lockCommand := command.(*protocol.LockCommand)

		if lockCommand.DbId == 0xff {
			err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.FreeLockCommand(lockCommand)
			return err
		}

		db := self.slock.dbs[lockCommand.DbId]
		if db == nil {
			db = self.slock.GetOrNewDB(lockCommand.DbId)
		}
		return db.Lock(self, lockCommand)

	case protocol.COMMAND_UNLOCK:
		lockCommand := command.(*protocol.LockCommand)

		if lockCommand.DbId == 0xff {
			err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.FreeLockCommand(lockCommand)
			return err
		}

		db := self.slock.dbs[lockCommand.DbId]
		if db == nil {
			err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
			_ = self.FreeLockCommand(lockCommand)
			return err
		}
		return db.UnLock(self, lockCommand)

	default:
		switch command.GetCommandType() {
		case protocol.COMMAND_INIT:
			initCommand := command.(*protocol.InitCommand)
			if self.Init(initCommand.ClientId) != nil {
				return self.Write(protocol.NewInitResultCommand(initCommand, protocol.RESULT_ERROR, 0))
			}
			self.slock.clientsGlock.Lock()
			initType := uint8(0)
			if _, ok := self.slock.clients[initCommand.ClientId]; ok {
				initType = 1
			}
			self.slock.clients[initCommand.ClientId] = self
			self.slock.clientsGlock.Unlock()
			return self.Write(protocol.NewInitResultCommand(initCommand, protocol.RESULT_SUCCEED, initType))

		case protocol.CommandState:
			return self.slock.GetState(self, command.(*protocol.StateCommand))

		case protocol.COMMAND_ADMIN:
			adminCommand := command.(*protocol.AdminCommand)
			err := self.Write(protocol.NewAdminResultCommand(adminCommand, protocol.RESULT_SUCCEED))
			if err != nil {
				return err
			}

			serverProtocol := NewTextServerProtocol(self.slock, self.stream)
			err = serverProtocol.Process()
			if err != nil {
				if err != io.EOF {
					self.slock.Log().Errorf("Protocol text connection process error %s %v", self.RemoteAddr().String(), err)
				}
			}

			if self.stream != nil {
				self.stream.protocol = self
			}
			self.totalCommandCount += serverProtocol.totalCommandCount
			serverProtocol.UnInitLockCommand()
			serverProtocol.closed = true
			return err

		case protocol.COMMAND_PING:
			pingCommand := command.(*protocol.PingCommand)
			return self.Write(protocol.NewPingResultCommand(pingCommand, protocol.RESULT_SUCCEED))

		case protocol.COMMAND_QUIT:
			quitCommand := command.(*protocol.QuitCommand)
			err := self.Write(protocol.NewQuitResultCommand(quitCommand, protocol.RESULT_SUCCEED))
			if err == nil {
				return io.EOF
			}
			return err

		case protocol.COMMAND_CALL:
			callCommand := command.(*protocol.CallCommand)
			return self.Write(protocol.NewCallResultCommand(callCommand, protocol.RESULT_UNKNOWN_COMMAND, "", nil))

		case protocol.COMMAND_WILL_LOCK:
			if self.willCommands == nil {
				self.glock.Lock()
				if self.willCommands == nil {
					self.willCommands = NewLockCommandQueue(2, 4, 8)
				}
				self.glock.Unlock()
			}
			lockCommand := command.(*protocol.LockCommand)
			lockCommand.CommandType = protocol.COMMAND_LOCK
			return self.willCommands.Push(lockCommand)

		case protocol.COMMAND_WILL_UNLOCK:
			if self.willCommands == nil {
				self.glock.Lock()
				if self.willCommands == nil {
					self.willCommands = NewLockCommandQueue(2, 4, 8)
				}
				self.glock.Unlock()
			}
			lockCommand := command.(*protocol.LockCommand)
			lockCommand.CommandType = protocol.COMMAND_UNLOCK
			return self.willCommands.Push(lockCommand)

		default:
			return self.Write(protocol.NewResultCommand(command, protocol.RESULT_UNKNOWN_COMMAND))
		}
	}
}

func (self *TextServerProtocol) ProcessLockCommand(lockCommand *protocol.LockCommand) error {
	if lockCommand.DbId == 0xff {
		return self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
	}

	db := self.slock.dbs[lockCommand.DbId]
	if lockCommand.CommandType == protocol.COMMAND_LOCK {
		if db == nil {
			db = self.slock.GetOrNewDB(lockCommand.DbId)
		}
		return db.Lock(self, lockCommand)
	}

	if db == nil {
		err := self.ProcessLockResultCommand(lockCommand, protocol.RESULT_UNKNOWN_DB, 0, 0)
		_ = self.FreeLockCommand(lockCommand)
		return err
	}
	return db.UnLock(self, lockCommand)
}

func (self *TextServerProtocol) ProcessLockResultCommand(lockCommand *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	self.lockRequestId[0], self.lockRequestId[1], self.lockRequestId[2], self.lockRequestId[3], self.lockRequestId[4], self.lockRequestId[5], self.lockRequestId[6], self.lockRequestId[7],
		self.lockRequestId[8], self.lockRequestId[9], self.lockRequestId[10], self.lockRequestId[11], self.lockRequestId[12], self.lockRequestId[13], self.lockRequestId[14], self.lockRequestId[15] =
		0, 0, 0, 0, 0, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 0, 0

	if self.freeCommandResult == nil {
		lockResultCommad := protocol.NewLockResultCommand(lockCommand, result, 0, lcount, lockCommand.Count, lrcount, lockCommand.Rcount)
		self.lockWaiter <- lockResultCommad
		return nil
	}

	lockResultCommad := self.freeCommandResult
	lockResultCommad.CommandType = lockCommand.CommandType
	lockResultCommad.RequestId = lockCommand.RequestId
	lockResultCommad.Result = result
	lockResultCommad.Flag = 0
	lockResultCommad.DbId = lockCommand.DbId
	lockResultCommad.LockId = lockCommand.LockId
	lockResultCommad.LockKey = lockCommand.LockKey
	lockResultCommad.Lcount = lcount
	lockResultCommad.Count = lockCommand.Count
	lockResultCommad.Lrcount = lrcount
	lockResultCommad.Rcount = lockCommand.Rcount
	self.freeCommandResult = nil
	self.lockWaiter <- lockResultCommad
	return nil
}

func (self *TextServerProtocol) ProcessLockResultCommandLocked(command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	self.glock.Lock()
	if command.RequestId != self.lockRequestId {
		self.glock.Unlock()
		return nil
	}

	err := self.ProcessLockResultCommand(command, result, lcount, lrcount)
	self.glock.Unlock()
	return err
}

func (self *TextServerProtocol) GetStream() *Stream {
	return self.stream
}

func (self *TextServerProtocol) GetProxy() *ServerProtocolProxy {
	return self.proxys[0]
}

func (self *TextServerProtocol) AddProxy(proxy *ServerProtocolProxy) error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return errors.New("closed")
	}

	self.proxys = append(self.proxys, proxy)
	self.glock.Unlock()
	return nil
}

func (self *TextServerProtocol) RemoteAddr() net.Addr {
	if self.stream == nil {
		return &net.TCPAddr{IP: []byte("0.0.0.0"), Port: 0, Zone: ""}
	}
	return self.stream.RemoteAddr()
}

func (self *TextServerProtocol) InitLockCommand() {
	self.slock.freeLockCommandLock.Lock()
	lockCommand := self.slock.freeLockCommandQueue.PopRight()
	if lockCommand != nil {
		self.slock.freeLockCommandCount--
		self.freeCommands[self.freeCommandIndex] = lockCommand
		self.freeCommandIndex++
	}
	self.slock.freeLockCommandLock.Unlock()
}

func (self *TextServerProtocol) UnInitLockCommand() {
	self.slock.freeLockCommandLock.Lock()
	for self.freeCommandIndex > 0 {
		self.freeCommandIndex--
		command := self.freeCommands[self.freeCommandIndex]
		_ = self.slock.freeLockCommandQueue.Push(command)
		self.slock.freeLockCommandCount++
	}

	for {
		command := self.lockedFreeCommands.PopRight()
		if command == nil {
			break
		}
		_ = self.slock.freeLockCommandQueue.Push(command)
		self.slock.freeLockCommandCount++
	}
	self.slock.freeLockCommandLock.Unlock()
}

func (self *TextServerProtocol) GetLockCommand() *protocol.LockCommand {
	if self.freeCommandIndex > 0 {
		self.freeCommandIndex--
		return self.freeCommands[self.freeCommandIndex]
	}
	return self.GetLockCommandLocked()
}

func (self *TextServerProtocol) GetLockCommandLocked() *protocol.LockCommand {
	self.glock.Lock()
	lockCommand := self.lockedFreeCommands.PopRight()
	if lockCommand != nil {
		self.glock.Unlock()
		return lockCommand
	}
	self.glock.Unlock()

	self.slock.freeLockCommandLock.Lock()
	lockCommand = self.slock.freeLockCommandQueue.PopRight()
	if lockCommand != nil {
		self.slock.freeLockCommandCount--
		self.slock.freeLockCommandLock.Unlock()
		return lockCommand
	}
	self.slock.freeLockCommandLock.Unlock()
	return &protocol.LockCommand{Command: protocol.Command{Magic: protocol.MAGIC, Version: protocol.VERSION}}
}

func (self *TextServerProtocol) FreeLockCommand(command *protocol.LockCommand) error {
	if self.freeCommandIndex < FREE_COMMAND_MAX_SIZE {
		self.freeCommands[self.freeCommandIndex] = command
		self.freeCommandIndex++
		return nil
	}
	return self.FreeLockCommandLocked(command)
}

func (self *TextServerProtocol) FreeLockCommandLocked(command *protocol.LockCommand) error {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		self.slock.freeLockCommandLock.Lock()
		_ = self.slock.freeLockCommandQueue.Push(command)
		self.slock.freeLockCommandCount++
		self.slock.freeLockCommandLock.Unlock()
		return nil
	}
	_ = self.lockedFreeCommands.Push(command)
	self.glock.Unlock()
	return nil
}

func (self *TextServerProtocol) ArgsToLockComandParseId(argId string, lockId *[16]byte) {
	arg_len := len(argId)
	if arg_len == 16 {
		lockId[0], lockId[1], lockId[2], lockId[3], lockId[4], lockId[5], lockId[6], lockId[7],
			lockId[8], lockId[9], lockId[10], lockId[11], lockId[12], lockId[13], lockId[14], lockId[15] =
			byte(argId[0]), byte(argId[1]), byte(argId[2]), byte(argId[3]), byte(argId[4]), byte(argId[5]), byte(argId[6]),
			byte(argId[7]), byte(argId[8]), byte(argId[9]), byte(argId[10]), byte(argId[11]), byte(argId[12]), byte(argId[13]), byte(argId[14]), byte(argId[15])
	} else if arg_len > 16 {
		if arg_len == 32 {
			v, err := hex.DecodeString(argId)
			if err == nil {
				lockId[0], lockId[1], lockId[2], lockId[3], lockId[4], lockId[5], lockId[6], lockId[7],
					lockId[8], lockId[9], lockId[10], lockId[11], lockId[12], lockId[13], lockId[14], lockId[15] =
					v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
					v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
			} else {
				v := md5.Sum([]byte(argId))
				lockId[0], lockId[1], lockId[2], lockId[3], lockId[4], lockId[5], lockId[6], lockId[7],
					lockId[8], lockId[9], lockId[10], lockId[11], lockId[12], lockId[13], lockId[14], lockId[15] =
					v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
					v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
			}
		} else {
			v := md5.Sum([]byte(argId))
			lockId[0], lockId[1], lockId[2], lockId[3], lockId[4], lockId[5], lockId[6], lockId[7],
				lockId[8], lockId[9], lockId[10], lockId[11], lockId[12], lockId[13], lockId[14], lockId[15] =
				v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
				v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15]
		}
	} else {
		argIndex := 16 - arg_len
		for i := 0; i < 16; i++ {
			if i < argIndex {
				lockId[i] = 0
			} else {
				lockId[i] = argId[i-argIndex]
			}
		}
	}
}

func (self *TextServerProtocol) ArgsToLockComand(args []string) (*protocol.LockCommand, error) {
	if len(args) < 2 || len(args)%2 != 0 {
		return nil, errors.New("Command Parse Len error")
	}

	commandName := strings.ToUpper(args[0])
	command := self.GetLockCommand()
	command.Magic = protocol.MAGIC
	command.Version = protocol.VERSION
	if commandName == "UNLOCK" {
		command.CommandType = protocol.COMMAND_UNLOCK
	} else {
		command.CommandType = protocol.COMMAND_LOCK
	}
	command.RequestId = self.GetRequestId()
	command.DbId = self.dbId
	command.Flag = 0
	command.Timeout = 3
	command.TimeoutFlag = 0
	command.Expried = 60
	command.ExpriedFlag = 0
	command.Count = 0
	command.Rcount = 0
	self.ArgsToLockComandParseId(args[1], &command.LockKey)

	hasLockId := false
	for i := 2; i < len(args); i += 2 {
		switch strings.ToUpper(args[i]) {
		case "LOCK_ID":
			self.ArgsToLockComandParseId(args[i+1], &command.LockId)
			hasLockId = true
		case "FLAG":
			flag, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, errors.New("Command Parse FLAG error")
			}
			command.Flag = uint8(flag)
		case "TIMEOUT":
			timeout, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return nil, errors.New("Command Parse TIMEOUT error")
			}
			command.Timeout = uint16(timeout & 0xffff)
			command.TimeoutFlag = uint16(timeout >> 16 & 0xffff)
		case "EXPRIED":
			expried, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return nil, errors.New("Command Parse EXPRIED error")
			}
			command.Expried = uint16(expried & 0xffff)
			command.ExpriedFlag = uint16(expried >> 16 & 0xffff)
		case "COUNT":
			count, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, errors.New("Command Parse COUNT error")
			}
			if count > 0 {
				command.Count = uint16(count) - 1
			} else {
				command.Count = uint16(count)
			}
		case "RCOUNT":
			rcount, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, errors.New("Command Parse RCOUNT error")
			}
			if rcount > 0 {
				command.Rcount = uint8(rcount) - 1
			} else {
				command.Rcount = uint8(rcount)
			}
		case "WILL":
			willType, err := strconv.Atoi(args[i+1])
			if err != nil {
				return nil, errors.New("Command Parse WILL error")
			}
			if willType > 0 && commandName != "PUSH" {
				command.CommandType += 7
			}
		}
	}

	if !hasLockId {
		if commandName == "LOCK" {
			command.LockId = command.RequestId
		} else {
			command.LockId = self.lockId
		}
	}
	return command, nil
}

func (self *TextServerProtocol) commandHandlerUnknownCommand(_ *TextServerProtocol, _ []string) error {
	return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR Unknown Command", nil))
}

func (self *TextServerProtocol) commandHandlerSelectDB(_ *TextServerProtocol, args []string) error {
	if len(args) < 2 {
		return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR Command Parse Len error", nil))
	}

	dbId, err := strconv.Atoi(args[1])
	if err != nil {
		return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR Command Parse DB_ID error", nil))
	}
	self.dbId = uint8(dbId)
	return self.stream.WriteBytes(self.parser.BuildResponse(true, "OK", nil))
}

func (self *TextServerProtocol) commandHandlerLock(_ *TextServerProtocol, args []string) error {
	lockCommand, err := self.ArgsToLockComand(args)
	if err != nil {
		return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR "+err.Error(), nil))
	}

	if lockCommand.DbId == 0xff {
		_ = self.FreeLockCommand(lockCommand)
		return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR Uknown DB error", nil))
	}

	if lockCommand.CommandType == protocol.COMMAND_WILL_LOCK {
		if self.willCommands == nil {
			self.glock.Lock()
			if self.willCommands == nil {
				self.willCommands = NewLockCommandQueue(2, 4, 8)
			}
			self.glock.Unlock()
		}
		_ = self.willCommands.Push(lockCommand)
		return self.stream.WriteBytes(self.parser.BuildResponse(true, "OK", nil))
	}

	db := self.slock.dbs[lockCommand.DbId]
	if db == nil {
		db = self.slock.GetOrNewDB(lockCommand.DbId)
	}
	self.lockRequestId = lockCommand.RequestId
	err = db.Lock(self, lockCommand)
	if err != nil {
		self.lockRequestId[0], self.lockRequestId[1], self.lockRequestId[2], self.lockRequestId[3], self.lockRequestId[4], self.lockRequestId[5], self.lockRequestId[6], self.lockRequestId[7],
			self.lockRequestId[8], self.lockRequestId[9], self.lockRequestId[10], self.lockRequestId[11], self.lockRequestId[12], self.lockRequestId[13], self.lockRequestId[14], self.lockRequestId[15] =
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0
		return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR lock error", nil))
	}
	lockCommandResult := <-self.lockWaiter
	if lockCommandResult.Result == 0 {
		self.lockId = lockCommand.LockId
	}

	bufIndex := 0
	tr := ""

	wbuf := self.parser.GetWriteBuf()
	bufIndex += copy(wbuf[bufIndex:], []byte("*12\r\n"))

	tr = fmt.Sprintf("%d", lockCommandResult.Result)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	tr = protocol.ERROR_MSG[lockCommandResult.Result]
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$7\r\nLOCK_ID\r\n$32\r\n"))
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("%x", lockCommandResult.LockId)))
	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$6\r\nLCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Lcount)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$5\r\nCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Count+1)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$7\r\nLRCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Lrcount)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$6\r\nRCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Rcount+1)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n"))

	self.freeCommandResult = lockCommandResult
	return self.stream.WriteBytes(wbuf[:bufIndex])
}

func (self *TextServerProtocol) commandHandlerUnlock(_ *TextServerProtocol, args []string) error {
	lockCommand, err := self.ArgsToLockComand(args)
	if err != nil {
		return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR "+err.Error(), nil))
	}

	if lockCommand.DbId == 0xff {
		_ = self.FreeLockCommand(lockCommand)
		return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR Uknown DB error", nil))
	}

	if lockCommand.CommandType == protocol.COMMAND_WILL_UNLOCK {
		if self.willCommands == nil {
			self.glock.Lock()
			if self.willCommands == nil {
				self.willCommands = NewLockCommandQueue(2, 4, 8)
			}
			self.glock.Unlock()
		}
		_ = self.willCommands.Push(lockCommand)
		return self.stream.WriteBytes(self.parser.BuildResponse(true, "OK", nil))
	}

	db := self.slock.dbs[lockCommand.DbId]
	if db == nil {
		_ = self.FreeLockCommand(lockCommand)
		return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR Uknown DB error", nil))
	}

	self.lockRequestId = lockCommand.RequestId
	err = db.UnLock(self, lockCommand)
	if err != nil {
		self.lockRequestId[0], self.lockRequestId[1], self.lockRequestId[2], self.lockRequestId[3], self.lockRequestId[4], self.lockRequestId[5], self.lockRequestId[6], self.lockRequestId[7],
			self.lockRequestId[8], self.lockRequestId[9], self.lockRequestId[10], self.lockRequestId[11], self.lockRequestId[12], self.lockRequestId[13], self.lockRequestId[14], self.lockRequestId[15] =
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0
		return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR UnLock error", nil))
	}
	lockCommandResult := <-self.lockWaiter
	if lockCommandResult.Result == 0 {
		self.lockId[0], self.lockId[1], self.lockId[2], self.lockId[3], self.lockId[4], self.lockId[5], self.lockId[6], self.lockId[7],
			self.lockId[8], self.lockId[9], self.lockId[10], self.lockId[11], self.lockId[12], self.lockId[13], self.lockId[14], self.lockId[15] =
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0
	}

	bufIndex := 0
	tr := ""

	wbuf := self.parser.GetWriteBuf()
	bufIndex += copy(wbuf[bufIndex:], []byte("*12\r\n"))

	tr = fmt.Sprintf("%d", lockCommandResult.Result)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	tr = protocol.ERROR_MSG[lockCommandResult.Result]
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$7\r\nLOCK_ID\r\n$32\r\n"))
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("%x", lockCommandResult.LockId)))
	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$6\r\nLCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Lcount)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$5\r\nCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Count+1)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$7\r\nLRCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Lrcount)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n$6\r\nRCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Rcount+1)
	bufIndex += copy(wbuf[bufIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufIndex += copy(wbuf[bufIndex:], []byte(tr))

	bufIndex += copy(wbuf[bufIndex:], []byte("\r\n"))

	self.freeCommandResult = lockCommandResult
	return self.stream.WriteBytes(wbuf[:bufIndex])
}

func (self *TextServerProtocol) commandHandlerPush(_ *TextServerProtocol, args []string) error {
	lockCommand, err := self.ArgsToLockComand(args)
	if err != nil {
		return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR "+err.Error(), nil))
	}

	if lockCommand.DbId == 0xff {
		_ = self.FreeLockCommand(lockCommand)
		return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR Uknown DB error", nil))
	}

	db := self.slock.dbs[lockCommand.DbId]
	if db == nil {
		db = self.slock.GetOrNewDB(lockCommand.DbId)
	}
	err = db.Lock(self, lockCommand)
	if err != nil {
		return self.stream.WriteBytes(self.parser.BuildResponse(false, "ERR lock error", nil))
	}
	return self.stream.WriteBytes(self.parser.BuildResponse(true, "OK", nil))
}

func (self *TextServerProtocol) GetRequestId() [16]byte {
	return protocol.GenRequestId()
}
