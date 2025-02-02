package server

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/snower/slock/client/godev"
	"github.com/snower/slock/protocol"
	"github.com/snower/slock/protocol/protobuf"
	"google.golang.org/protobuf/proto"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type ReplicationBufferQueue struct {
	manager         *ReplicationManager
	glock           *sync.RWMutex
	buf             []byte
	segmentCount    uint64
	segmentSize     uint64
	currentIndex    uint64
	maxIndex        uint64
	maxBufferSize   uint64
	dupCurrentIndex uint64
	dupSegmentCount uint64
	dupCount        uint64
	dupGlock        *sync.Mutex
	dupWaiter       chan bool
	dupPulled       bool
	closed          bool
}

func NewReplicationBufferQueue(manager *ReplicationManager, bufSize uint64, maxSize uint64) *ReplicationBufferQueue {
	maxIndex := uint64(0xffffffffffffffff) - uint64(0xffffffffffffffff)%(bufSize/64)
	queue := &ReplicationBufferQueue{manager, &sync.RWMutex{}, make([]byte, bufSize),
		bufSize / 64, 64, 0, maxIndex, maxSize, 0xffffffffffffffff,
		bufSize / 128, 0, &sync.Mutex{}, make(chan bool, 1), false, false}
	return queue
}

func (self *ReplicationBufferQueue) Reduplicated() *ReplicationBufferQueue {
	bufSize := self.segmentCount * 2 * self.segmentSize
	self.dupGlock.Lock()
	if bufSize > self.maxBufferSize {
		self.dupPulled = true
		self.dupGlock.Unlock()
		select {
		case <-self.dupWaiter:
			self.dupCount++
		case <-time.After(10 * time.Millisecond):
			self.dupCount++
		}
		return self
	}

	buf := make([]byte, bufSize)
	copy(buf, self.buf)
	copy(buf[self.segmentCount*self.segmentSize:], self.buf)
	self.buf = buf
	self.segmentCount = bufSize / 64
	self.maxIndex = uint64(0xffffffffffffffff) - uint64(0xffffffffffffffff)%bufSize/64
	self.dupSegmentCount = bufSize / 128
	self.dupGlock.Unlock()
	self.manager.slock.Log().Infof("Replication aof ring buffer reduplicated size %d to %d", bufSize/2, bufSize)
	self.dupCount++
	return self
}

func (self *ReplicationBufferQueue) WakeupReduplicated() {
	if self.dupCurrentIndex < 0xffffffffffffffff {
		if self.currentIndex >= self.dupCurrentIndex {
			if self.currentIndex-self.dupCurrentIndex > self.dupSegmentCount {
				return
			}
		} else {
			if self.maxIndex-self.dupCurrentIndex+(self.currentIndex-self.segmentCount) > self.dupSegmentCount {
				return
			}
		}
	}

	self.dupGlock.Lock()
	if self.dupPulled {
		self.dupWaiter <- true
		self.dupPulled = false
	}
	self.dupGlock.Unlock()
}

func (self *ReplicationBufferQueue) Close() error {
	self.dupGlock.Lock()
	if self.dupPulled {
		self.dupWaiter <- false
		self.dupPulled = false
	}
	self.closed = true
	self.dupGlock.Unlock()
	return nil
}

func (self *ReplicationBufferQueue) Push(buf []byte) error {
	if self.dupCurrentIndex < 0xffffffffffffffff {
		if self.currentIndex >= self.dupCurrentIndex {
			if self.currentIndex-self.dupCurrentIndex > self.dupSegmentCount {
				self.Reduplicated()
			}
		} else {
			if self.maxIndex-self.dupCurrentIndex+(self.currentIndex-self.segmentCount) > self.dupSegmentCount {
				self.Reduplicated()
			}
		}
	}

	self.glock.Lock()
	currentSize := (self.currentIndex % self.segmentCount) * self.segmentSize
	copy(self.buf[currentSize:], buf)
	self.currentIndex++
	if self.currentIndex >= self.maxIndex {
		self.currentIndex = self.segmentCount
	}
	self.glock.Unlock()
	return nil
}

func (self *ReplicationBufferQueue) Pop(segmentIndex uint64, buf []byte) error {
	self.glock.RLock()
	if segmentIndex == self.currentIndex || self.currentIndex == 0 {
		self.glock.RUnlock()
		return io.EOF
	}

	if self.currentIndex >= segmentIndex {
		if self.currentIndex-segmentIndex > self.segmentCount {
			self.glock.RUnlock()
			return errors.New("segment out of buf")
		}
	} else {
		if self.maxIndex-segmentIndex+(self.currentIndex-self.segmentCount) > self.segmentCount {
			self.glock.RUnlock()
			return errors.New("segment out of buf")
		}
	}

	currentSize := (segmentIndex % self.segmentCount) * self.segmentSize
	copy(buf, self.buf[currentSize:currentSize+self.segmentSize])
	self.glock.RUnlock()
	return nil
}

func (self *ReplicationBufferQueue) Head(buf []byte) (uint64, error) {
	self.glock.RLock()
	if self.currentIndex == 0 {
		self.glock.RUnlock()
		return 0, errors.New("buffer is empty")
	}

	currentSize := ((self.currentIndex - 1) % self.segmentCount) * self.segmentSize
	copy(buf, self.buf[currentSize:currentSize+self.segmentSize])
	self.glock.RUnlock()
	return self.currentIndex, nil
}

func (self *ReplicationBufferQueue) Search(aofId [16]byte, aofBuf []byte) (uint64, error) {
	self.glock.RLock()
	if self.currentIndex == 0 {
		self.glock.RUnlock()
		return 0, errors.New("search error")
	}

	startIndex := uint64(0)
	segmentCount := self.segmentCount
	if self.currentIndex > self.segmentCount {
		startIndex = self.currentIndex - self.segmentCount
	} else {
		segmentCount = self.currentIndex
	}

	for i := segmentCount; i > 0; i-- {
		currentSize := ((startIndex + i - 1) % self.segmentCount) * self.segmentSize
		buf := self.buf[currentSize : currentSize+self.segmentSize]
		currentAofId := [16]byte{buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]}
		if currentAofId == aofId {
			copy(aofBuf, buf)
			currentAofId := [16]byte{aofBuf[3], aofBuf[4], aofBuf[5], aofBuf[6], aofBuf[7], aofBuf[8], aofBuf[9], aofBuf[10], aofBuf[11], aofBuf[12], aofBuf[13], aofBuf[14], aofBuf[15], aofBuf[16], aofBuf[17], aofBuf[18]}
			if currentAofId == aofId {
				self.glock.RUnlock()
				return startIndex + i - 1, nil
			}
		}
	}

	self.glock.RUnlock()
	return 0, errors.New("search error")
}

type ReplicationClient struct {
	manager          *ReplicationManager
	glock            *sync.Mutex
	stream           *godev.Stream
	protocol         *godev.BinaryClientProtocol
	aof              *Aof
	aofLock          *AofLock
	currentRequestId [16]byte
	rbufs            [][]byte
	rbufIndex        int
	rbufChannel      chan []byte
	wbuf             []byte
	loadedCount      uint64
	wakeupSignal     chan bool
	closedWaiter     chan bool
	closed           bool
	connectedLeader  bool
	recvedFiles      bool
}

func NewReplicationClient(manager *ReplicationManager) *ReplicationClient {
	channel := &ReplicationClient{manager, &sync.Mutex{}, nil, nil, manager.slock.GetAof(),
		nil, [16]byte{}, make([][]byte, 16), 0, make(chan []byte, 8),
		make([]byte, 64), 0, nil, make(chan bool, 1), false, true, false}
	for i := 0; i < 16; i++ {
		channel.rbufs[i] = make([]byte, 64)
	}
	return channel
}

func (self *ReplicationClient) Open(addr string) error {
	if self.protocol != nil {
		return errors.New("Client is Opened")
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return err
	}
	stream := godev.NewStream(conn)
	clientProtocol := godev.BuildNewBinaryClientProtocol(stream)
	self.stream = stream
	self.protocol = clientProtocol
	self.closed = false
	return nil
}

func (self *ReplicationClient) Close() error {
	self.closed = true
	if self.protocol != nil {
		_ = self.protocol.close()
	}
	_ = self.WakeupRetryConnect()
	self.manager.slock.logger.Infof("Replication client %s close", self.manager.leaderAddress)
	return nil
}

func (self *ReplicationClient) Run() {
	self.currentRequestId = self.manager.currentRequestId
	for !self.closed {
		self.manager.slock.logger.Infof("Replication client connect leader %s", self.manager.leaderAddress)
		err := self.Open(self.manager.leaderAddress)
		if err != nil {
			self.manager.slock.logger.Errorf("Replication client connect leader %s error %v", self.manager.leaderAddress, err)
			if self.protocol != nil {
				_ = self.protocol.close()
			}
			self.glock.Lock()
			self.stream = nil
			self.protocol = nil
			self.glock.Unlock()
			self.manager.wakeupInitSyncedWaiters()
			if self.closed {
				break
			}
			_ = self.sleepWhenRetryConnect()
			continue
		}

		err = self.InitSync()
		if err != nil {
			if err != io.EOF {
				self.manager.slock.logger.Errorf("Replication client init sync error %s %v", self.manager.leaderAddress, err)
			}
		} else {
			self.manager.clientSycnInited()
			self.manager.slock.logger.Infof("Replication client connected leader %s", self.manager.leaderAddress)
			err = self.Process()
			if err != nil {
				if err != io.EOF && !self.closed {
					self.manager.slock.logger.Errorf("Replication client sync leader %s error %v", self.manager.leaderAddress, err)
				}
			}
		}

		if self.protocol != nil {
			_ = self.protocol.close()
		}
		self.glock.Lock()
		self.stream = nil
		self.protocol = nil
		self.glock.Unlock()
		self.manager.wakeupInitSyncedWaiters()
		if self.closed {
			break
		}
		_ = self.sleepWhenRetryConnect()
	}

	close(self.closedWaiter)
	self.manager.clientChannel = nil
	self.manager.currentRequestId = self.currentRequestId
	self.manager.slock.logger.Infof("Replication client connect leader %s closed", self.manager.leaderAddress)
}

func (self *ReplicationClient) sendSyncCommand() (*protobuf.SyncResponse, error) {
	requestId := fmt.Sprintf("%x", self.currentRequestId)
	if requestId != "00000000000000000000000000000000" {
		if self.aofLock == nil {
			self.aofLock = NewAofLock()
		}
		self.manager.slock.logger.Infof("Replication client send start sync %s", requestId)
	} else {
		requestId = ""
		self.manager.slock.logger.Infof("Replication client send start sync")
	}

	request := protobuf.SyncRequest{AofId: requestId}
	data, err := proto.Marshal(&request)
	if err != nil {
		return nil, err
	}
	command := protocol.NewCallCommand("SYNC", data)
	werr := self.protocol.write(command)
	if werr != nil {
		return nil, werr
	}

	resultCommand, rerr := self.protocol.read()
	if rerr != nil {
		return nil, rerr
	}

	callResultCommand, ok := resultCommand.(*protocol.CallResultCommand)
	if !ok {
		return nil, errors.New("unknown command result")
	}

	if callResultCommand.Result != 0 || callResultCommand.ErrType != "" {
		if callResultCommand.Result == 0 && callResultCommand.ErrType == "ERR_NOT_FOUND" {
			self.currentRequestId = [16]byte{}
			self.manager.slock.logger.Infof("Replication client resend file sync all data")
			self.aofLock = nil
			self.recvedFiles = false
			return self.sendSyncCommand()
		}
		return nil, errors.New(callResultCommand.ErrType)
	}

	response := protobuf.SyncResponse{}
	err = proto.Unmarshal(callResultCommand.Data, &response)
	if err != nil {
		return nil, errors.New("unknown lastest requestid")
	}
	self.manager.slock.logger.Infof("Replication client recv start sync aof_id %s", response.AofId)
	return &response, nil
}

func (self *ReplicationClient) InitSync() error {
	syncResponse, err := self.sendSyncCommand()
	if err != nil {
		return err
	}

	if self.aofLock != nil {
		err = self.sendStarted()
		if err != nil {
			return err
		}
		self.recvedFiles = true
		self.manager.slock.logger.Infof("Replication client start sync, waiting from aof_id %x", self.currentRequestId)
		return nil
	}

	buf, err := hex.DecodeString(syncResponse.AofId)
	if err != nil {
		return err
	}
	aofFileIndex := uint32(buf[4]) | uint32(buf[5])<<8 | uint32(buf[6])<<16 | uint32(buf[7])<<24
	if aofFileIndex > 0 {
		aofFileIndex = aofFileIndex - 1
	}
	err = self.aof.Reset(aofFileIndex)
	if err != nil {
		return err
	}
	err = self.manager.FlushDB()
	if err != nil {
		return err
	}

	self.aofLock = NewAofLock()
	err = self.sendStarted()
	if err != nil {
		return err
	}

	self.currentRequestId[0], self.currentRequestId[1], self.currentRequestId[2], self.currentRequestId[3], self.currentRequestId[4], self.currentRequestId[5], self.currentRequestId[6], self.currentRequestId[7],
		self.currentRequestId[8], self.currentRequestId[9], self.currentRequestId[10], self.currentRequestId[11], self.currentRequestId[12], self.currentRequestId[13], self.currentRequestId[14], self.currentRequestId[15] = buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
		buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15]
	self.manager.slock.logger.Infof("Replication client start recv files util aof_id %x", self.currentRequestId)
	return self.recvFiles()
}

func (self *ReplicationClient) sendStarted() error {
	aofLock := NewAofLock()
	aofLock.CommandType = protocol.COMMAND_INIT
	aofLock.AofIndex = 0xffffffff
	aofLock.AofId = 0xffffffff
	aofLock.CommandTime = 0xffffffffffffffff
	err := aofLock.Encode()
	if err != nil {
		return err
	}
	self.glock.Lock()
	err = self.stream.WriteBytes(aofLock.buf)
	self.glock.Unlock()
	if err != nil {
		return err
	}
	return nil
}

func (self *ReplicationClient) recvFiles() error {
	defer func() {
		self.aof.glock.Lock()
		self.aof.isRewriting = false
		if self.aof.rewritedWaiter != nil {
			close(self.aof.rewritedWaiter)
			self.aof.rewritedWaiter = nil
		}
		self.aof.glock.Unlock()
	}()
	_ = self.aof.WaitRewriteAofFiles()
	self.aof.glock.Lock()
	self.aof.isRewriting = true
	self.aof.glock.Unlock()

	var aofFile *AofFile = nil
	aofIndex := uint32(0)
	for !self.closed {
		err := self.readLock()
		if err != nil {
			return err
		}

		if self.aofLock.CommandType == protocol.COMMAND_INIT && self.aofLock.AofIndex == 0xffffffff &&
			self.aofLock.AofId == 0xffffffff && self.aofLock.CommandTime == 0xffffffffffffffff {
			if aofFile != nil {
				err := aofFile.Flush()
				if err != nil {
					self.manager.slock.logger.Errorf("Replication client flush aof file %s error %v", aofFile.filename, err)
				}
				err = aofFile.Close()
				if err != nil {
					self.manager.slock.logger.Errorf("Replication client close aof file %s error %v", aofFile.filename, err)
					return err
				}
			}
			self.recvedFiles = true
			self.manager.slock.logger.Infof("Replication client recv files finish, current aof_id %x", self.currentRequestId)
			return nil
		}

		currentAofIndex := self.aofLock.AofIndex
		if self.aofLock.AofFlag&AOF_FLAG_REWRITEd != 0 {
			currentAofIndex = 0
		}
		if currentAofIndex != aofIndex || aofFile == nil {
			if aofFile != nil {
				err := aofFile.Flush()
				if err != nil {
					self.manager.slock.logger.Errorf("Replication client flush aof file %s error %v", aofFile.filename, err)
				}
				err = aofFile.Close()
				if err != nil {
					self.manager.slock.logger.Errorf("Replication client close aof file %s error %v", aofFile.filename, err)
					return err
				}
			}

			aofFile, err = self.aof.OpenAofFile(currentAofIndex)
			if err != nil {
				return err
			}
			aofIndex = currentAofIndex
			if aofIndex == 0 {
				self.manager.slock.logger.Infof("Replication client recv file rewrite.aof")
			} else {
				self.manager.slock.logger.Infof(fmt.Sprintf("Replication client recv file %s.%d", "append.aof", aofIndex))
			}
		}

		err = self.aof.LoadLock(self.aofLock)
		if err != nil {
			return err
		}
		err = aofFile.AppendLock(self.aofLock)
		if err != nil {
			return err
		}
		self.loadedCount++

		buf := self.aofLock.buf
		self.currentRequestId[0], self.currentRequestId[1], self.currentRequestId[2], self.currentRequestId[3], self.currentRequestId[4], self.currentRequestId[5], self.currentRequestId[6], self.currentRequestId[7],
			self.currentRequestId[8], self.currentRequestId[9], self.currentRequestId[10], self.currentRequestId[11], self.currentRequestId[12], self.currentRequestId[13], self.currentRequestId[14], self.currentRequestId[15] = buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]
	}
	return io.EOF
}

func (self *ReplicationClient) Process() error {
	go self.readProcess()
	aof := self.aof
	bufferQueue := self.manager.bufferQueue

	for !self.closed {
		err := self.getLock()
		if err != nil {
			return err
		}

		err = aof.ReplayLock(self.aofLock)
		if err != nil {
			return err
		}
		aof.AppendLock(self.aofLock)
		_ = bufferQueue.Push(self.aofLock.buf)
		self.loadedCount++

		buf := self.aofLock.buf
		self.currentRequestId[0], self.currentRequestId[1], self.currentRequestId[2], self.currentRequestId[3], self.currentRequestId[4], self.currentRequestId[5], self.currentRequestId[6], self.currentRequestId[7],
			self.currentRequestId[8], self.currentRequestId[9], self.currentRequestId[10], self.currentRequestId[11], self.currentRequestId[12], self.currentRequestId[13], self.currentRequestId[14], self.currentRequestId[15] = buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]
	}
	return io.EOF
}

func (self *ReplicationClient) readLock() error {
	buf := self.aofLock.buf
	n, err := self.stream.Read(buf)
	if err != nil {
		return err
	}

	if n < 64 {
		for n < 64 {
			nn, nerr := self.stream.Read(buf[n:])
			if nerr != nil {
				return nerr
			}
			n += nn
		}
	}

	err = self.aofLock.Decode()
	if err != nil {
		return err
	}
	return nil
}

func (self *ReplicationClient) getLock() error {
	for {
		select {
		case buf := <-self.rbufChannel:
			if buf == nil {
				self.aof.aofGlock.Lock()
				if self.aof.aofFile.windex > 0 || self.aof.aofFile.dirtied {
					self.aof.Flush()
				}
				self.aof.aofGlock.Unlock()
				return io.EOF
			}

			self.aofLock.buf = buf
			err := self.aofLock.Decode()
			if err != nil {
				return err
			}
			return nil
		default:
			if self.closed {
				self.aof.aofGlock.Lock()
				if self.aof.aofFile.windex > 0 || self.aof.aofFile.dirtied {
					self.aof.Flush()
				}
				self.aof.aofGlock.Unlock()
				return io.EOF
			}

			self.aof.aofGlock.Lock()
			if self.aof.aofFile.windex > 0 && self.aof.aofFile.ackIndex > 0 {
				err := self.aof.aofFile.Flush()
				if err != nil {
					self.manager.slock.Log().Errorf("Replication flush file error %v", err)
				}
			}
			self.aof.aofGlock.Unlock()

			select {
			case buf := <-self.rbufChannel:
				if buf == nil {
					self.aof.aofGlock.Lock()
					if self.aof.aofFile.windex > 0 || self.aof.aofFile.dirtied {
						self.aof.Flush()
					}
					self.aof.aofGlock.Unlock()
					return io.EOF
				}

				self.aofLock.buf = buf
				err := self.aofLock.Decode()
				if err != nil {
					return err
				}
				return nil
			case <-time.After(200 * time.Millisecond):
				self.aof.aofGlock.Lock()
				if self.aof.aofFile.windex > 0 || self.aof.aofFile.dirtied {
					self.aof.Flush()
				}
				self.aof.aofGlock.Unlock()
				buf := <-self.rbufChannel
				if buf == nil {
					return io.EOF
				}

				self.aofLock.buf = buf
				err := self.aofLock.Decode()
				if err != nil {
					return err
				}
				return nil
			}
		}
	}
}

func (self *ReplicationClient) readProcess() {
	for !self.closed {
		buf := self.rbufs[self.rbufIndex]
		n, err := self.stream.Read(buf)
		if err != nil {
			self.rbufChannel <- nil
			return
		}

		if n < 64 {
			for n < 64 {
				nn, nerr := self.stream.Read(buf[n:])
				if nerr != nil {
					self.rbufChannel <- nil
					return
				}
				n += nn
			}
		}

		self.rbufChannel <- buf
		self.rbufIndex++
		if self.rbufIndex >= len(self.rbufs) {
			self.rbufIndex = 0
		}
	}

	self.rbufChannel <- nil
}

func (self *ReplicationClient) HandleAcked(ackLock *ReplicationAckLock) error {
	if ackLock.aofResult != 0 && ackLock.lockResult.Result == 0 {
		ackLock.lockResult.Result = protocol.RESULT_ERROR
	}
	err := ackLock.lockResult.Encode(self.wbuf)
	if err != nil {
		return err
	}

	self.glock.Lock()
	if self.stream == nil {
		self.glock.Unlock()
		return errors.New("stream closed")
	}
	err = self.stream.WriteBytes(self.wbuf)
	self.glock.Unlock()
	return err
}

func (self *ReplicationClient) sleepWhenRetryConnect() error {
	self.glock.Lock()
	self.wakeupSignal = make(chan bool, 1)
	self.glock.Unlock()

	select {
	case <-self.wakeupSignal:
		return nil
	case <-time.After(5 * time.Second):
		self.glock.Lock()
		self.wakeupSignal = nil
		self.glock.Unlock()
		return nil
	}
}

func (self *ReplicationClient) WakeupRetryConnect() error {
	self.glock.Lock()
	if self.wakeupSignal != nil {
		close(self.wakeupSignal)
		self.wakeupSignal = nil
	}
	self.glock.Unlock()
	return nil
}

type ReplicationServer struct {
	manager          *ReplicationManager
	stream           *Stream
	protocol         *BinaryServerProtocol
	aof              *Aof
	raofLock         *AofLock
	waofLock         *AofLock
	currentRequestId [16]byte
	bufferIndex      uint64
	pulled           uint32
	pulledWaiter     chan bool
	wakeupedBuffer   bool
	closed           bool
	closedWaiter     chan bool
	sendedFiles      bool
}

func NewReplicationServer(manager *ReplicationManager, serverProtocol *BinaryServerProtocol) *ReplicationServer {
	return &ReplicationServer{manager, serverProtocol.stream, serverProtocol,
		manager.slock.GetAof(), NewAofLock(), NewAofLock(),
		[16]byte{}, 0, 0, make(chan bool, 1),
		false, false, make(chan bool, 1), false}
}

func (self *ReplicationServer) Close() error {
	self.closed = true
	if self.protocol != nil {
		_ = self.protocol.Close()
	}
	self.manager.slock.Log().Infof("Replication server %s close", self.protocol.RemoteAddr().String())
	return nil
}

func (self *ReplicationServer) handleInitSync(command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.manager.slock.state != STATE_LEADER {
		return protocol.NewCallResultCommand(command, 0, "ERR_STATE", nil), nil
	}

	request := protobuf.SyncRequest{}
	err := proto.Unmarshal(command.Data, &request)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_PROTO", nil), nil
	}

	if request.AofId == "" {
		bufferIndex, err := self.manager.bufferQueue.Head(self.waofLock.buf)
		if err != nil {
			self.waofLock.AofIndex = self.aof.aofFileIndex
			self.waofLock.AofId = 0
		} else {
			err := self.waofLock.Decode()
			if err != nil {
				return protocol.NewCallResultCommand(command, 0, "ERR_DECODE", nil), nil
			}
		}
		requestId := fmt.Sprintf("%x", self.waofLock.GetRequestId())
		response := protobuf.SyncResponse{AofId: requestId}
		data, err := proto.Marshal(&response)
		if err != nil {
			return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
		}
		err = self.protocol.Write(protocol.NewCallResultCommand(command, 0, "", data))
		if err != nil {
			return nil, err
		}
		self.bufferIndex = bufferIndex
		self.manager.slock.logger.Infof("Replication server recv client %s send files start by aof_id %s", self.protocol.RemoteAddr().String(), requestId)

		err = self.waitStarted()
		if err != nil {
			return nil, err
		}
		go (func() {
			err := self.sendFiles()
			if err != nil {
				self.manager.slock.logger.Infof("Replication server handle client %s send files error %v", self.protocol.RemoteAddr().String(), err)
				_ = self.Close()
				return
			}
		})()
		return nil, nil
	}

	self.manager.slock.logger.Infof("Replication server recv client %s sync require start by aof_id %s", self.protocol.RemoteAddr().String(), request.AofId)
	buf, err := hex.DecodeString(request.AofId)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_AOF_ID", nil), nil
	}
	if buf[4] == 0 && buf[5] == 0 && buf[6] == 0 && buf[7] == 0 {
		return protocol.NewCallResultCommand(command, 0, "ERR_NOT_FOUND", nil), nil
	}
	initedAofId := [16]byte{buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15]}
	bufferIndex, serr := self.manager.bufferQueue.Search(initedAofId, self.waofLock.buf)
	if serr != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_NOT_FOUND", nil), nil
	}

	err = self.waofLock.Decode()
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
	}
	requestId := fmt.Sprintf("%x", self.waofLock.GetRequestId())
	response := protobuf.SyncResponse{AofId: requestId}
	data, err := proto.Marshal(&response)
	if err != nil {
		return protocol.NewCallResultCommand(command, 0, "ERR_ENCODE", nil), nil
	}
	err = self.protocol.Write(protocol.NewCallResultCommand(command, 0, "", data))

	if err != nil {
		return nil, err
	}
	self.bufferIndex = bufferIndex + 1
	self.sendedFiles = true
	self.manager.slock.logger.Infof("Replication server handle client %s send start by aof_id %s", self.protocol.RemoteAddr().String(), requestId)

	err = self.waitStarted()
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (self *ReplicationServer) sendFiles() error {
	_ = self.aof.WaitRewriteAofFiles()
	self.aof.aofGlock.Lock()
	self.aof.Flush()
	self.aof.aofGlock.Unlock()

	appendFiles, rewriteFile, err := self.aof.FindAofFiles()
	if err != nil {
		return err
	}

	aofFilenames := make([]string, 0)
	if rewriteFile != "" {
		aofFilenames = append(aofFilenames, rewriteFile)
	}
	aofFilenames = append(aofFilenames, appendFiles...)
	err = self.aof.LoadAofFiles(aofFilenames, time.Now().Unix(), func(filename string, aofFile *AofFile, lock *AofLock, firstLock bool) (bool, error) {
		if lock.AofIndex > self.waofLock.AofIndex && lock.AofId > self.waofLock.AofId {
			return false, nil
		}

		err := self.stream.WriteBytes(lock.buf)
		if err != nil {
			return true, err
		}
		self.currentRequestId = lock.GetRequestId()
		return true, nil
	})
	if err != nil {
		return err
	}

	err = self.sendFilesFinished()
	if err != nil {
		return err
	}
	self.manager.slock.logger.Infof("Replication server handle client %s send file finish, send queue by aof_id %x",
		self.protocol.RemoteAddr().String(), self.waofLock.GetRequestId())
	return self.manager.WakeupServerChannel()
}

func (self *ReplicationServer) waitStarted() error {
	buf := self.raofLock.buf
	for !self.closed {
		n, err := self.stream.Read(buf)
		if err != nil {
			return err
		}

		if n < 64 {
			for n < 64 {
				nn, nerr := self.stream.Read(buf[n:])
				if nerr != nil {
					return nerr
				}
				n += nn
			}
		}

		err = self.raofLock.Decode()
		if err != nil {
			return err
		}

		if self.raofLock.CommandType == protocol.COMMAND_INIT && self.raofLock.AofIndex == 0xffffffff &&
			self.raofLock.AofId == 0xffffffff && self.raofLock.CommandTime == 0xffffffffffffffff {
			return nil
		}
	}
	return io.EOF
}

func (self *ReplicationServer) sendFilesFinished() error {
	aofLock := NewAofLock()
	aofLock.CommandType = protocol.COMMAND_INIT
	aofLock.AofIndex = 0xffffffff
	aofLock.AofId = 0xffffffff
	aofLock.CommandTime = 0xffffffffffffffff
	err := aofLock.Encode()
	if err != nil {
		return err
	}
	err = self.stream.WriteBytes(aofLock.buf)
	if err != nil {
		return err
	}
	self.sendedFiles = true
	return nil
}

func (self *ReplicationServer) sendFilesQueue() error {
	bufs := make([][]byte, 0)
	atomic.AddUint32(&self.manager.serverActiveCount, 1)
	for !self.closed && !self.sendedFiles {
		buf := make([]byte, 64)
		err := self.manager.bufferQueue.Pop(self.bufferIndex, buf)
		if err != nil {
			if err == io.EOF {
				atomic.AddUint32(&self.pulled, 1)
				atomic.AddUint32(&self.manager.serverActiveCount, 0xffffffff)
				<-self.pulledWaiter
				atomic.AddUint32(&self.manager.serverActiveCount, 1)
				continue
			}
			atomic.AddUint32(&self.manager.serverActiveCount, 0xffffffff)
			return err
		}

		self.bufferIndex++
		if self.bufferIndex >= self.manager.bufferQueue.maxIndex {
			self.bufferIndex = uint64(self.manager.bufferQueue.segmentCount)
		}
		bufs = append(bufs, buf)
	}
	atomic.AddUint32(&self.manager.serverActiveCount, 0xffffffff)

	if !self.closed {
		for _, buf := range bufs {
			copy(self.waofLock.buf, buf)
			err := self.waofLock.Decode()
			if err != nil {
				return err
			}

			err = self.stream.WriteBytes(buf)
			if err != nil {
				return err
			}
			self.currentRequestId = self.waofLock.GetRequestId()
		}
	}
	return nil
}

func (self *ReplicationServer) SendProcess() error {
	if !self.sendedFiles {
		err := self.sendFilesQueue()
		if err != nil {
			return err
		}
	}

	atomic.AddUint32(&self.manager.serverActiveCount, 1)
	for !self.closed {
		bufferQueue := self.manager.bufferQueue
		buf := self.waofLock.buf
		err := bufferQueue.Pop(self.bufferIndex, buf)
		if err != nil {
			if err == io.EOF {
				atomic.AddUint32(&self.pulled, 1)
				atomic.AddUint32(&self.manager.serverActiveCount, 0xffffffff)
				if atomic.CompareAndSwapUint32(&self.manager.serverActiveCount, 0, 0) {
					self.manager.glock.Lock()
					if self.manager.serverFlushWaiter != nil {
						close(self.manager.serverFlushWaiter)
						self.manager.serverFlushWaiter = nil
					}
					self.manager.glock.Unlock()
				}
				if bufferQueue.dupPulled {
					bufferQueue.WakeupReduplicated()
				}
				self.wakeupedBuffer = false
				<-self.pulledWaiter
				atomic.AddUint32(&self.manager.serverActiveCount, 1)
				continue
			}
			atomic.AddUint32(&self.manager.serverActiveCount, 0xffffffff)
			return err
		}

		err = self.stream.WriteBytes(buf)
		if err != nil {
			atomic.AddUint32(&self.manager.serverActiveCount, 0xffffffff)
			return err
		}

		self.currentRequestId[0], self.currentRequestId[1], self.currentRequestId[2], self.currentRequestId[3], self.currentRequestId[4], self.currentRequestId[5], self.currentRequestId[6], self.currentRequestId[7],
			self.currentRequestId[8], self.currentRequestId[9], self.currentRequestId[10], self.currentRequestId[11], self.currentRequestId[12], self.currentRequestId[13], self.currentRequestId[14], self.currentRequestId[15] = buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
			buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]

		self.bufferIndex++
		if self.bufferIndex >= bufferQueue.maxIndex {
			bufferQueue.dupGlock.Lock()
			if self.bufferIndex >= bufferQueue.maxIndex {
				self.bufferIndex = bufferQueue.segmentCount
			}
			bufferQueue.dupGlock.Unlock()
		}
		if self.wakeupedBuffer {
			if bufferQueue.currentIndex >= self.bufferIndex {
				if bufferQueue.currentIndex-self.bufferIndex < bufferQueue.dupSegmentCount {
					if bufferQueue.dupPulled {
						bufferQueue.WakeupReduplicated()
					}
				}
			} else {
				if bufferQueue.maxIndex-self.bufferIndex+(bufferQueue.currentIndex-bufferQueue.segmentCount) < bufferQueue.dupSegmentCount {
					if bufferQueue.dupPulled {
						bufferQueue.WakeupReduplicated()
					}
				}
			}
			self.wakeupedBuffer = false
		}
	}
	atomic.AddUint32(&self.manager.serverActiveCount, 0xffffffff)
	return nil
}

func (self *ReplicationServer) RecvProcess() error {
	buf := self.raofLock.buf
	lockResult := &protocol.LockResultCommand{}
	for !self.closed {
		n, err := self.stream.Read(buf)
		if err != nil {
			return err
		}

		if n < 64 {
			for n < 64 {
				nn, nerr := self.stream.Read(buf[n:])
				if nerr != nil {
					return nerr
				}
				n += nn
			}
		}

		err = lockResult.Decode(buf)
		if err != nil {
			return err
		}

		err = self.aof.loadLockAck(lockResult)
		if err != nil {
			return err
		}
	}
	return io.EOF
}

type ReplicationAckLock struct {
	lockResult protocol.LockResultCommand
	aofResult  uint8
	locked     bool
	aofed      bool
}

func NewReplicationAckLock() *ReplicationAckLock {
	resultCommand := protocol.ResultCommand{Magic: protocol.MAGIC, Version: protocol.VERSION, CommandType: 0, RequestId: [16]byte{}, Result: 0}
	lockResult := protocol.LockResultCommand{ResultCommand: resultCommand, Flag: 0, DbId: 0, LockId: [16]byte{}, LockKey: [16]byte{},
		Count: 0, Lcount: 0, Lrcount: 0, Rcount: 0, Blank: protocol.RESULT_LOCK_COMMAND_BLANK_BYTERS}
	return &ReplicationAckLock{lockResult, 0, false, false}
}

type ReplicationAckDB struct {
	manager           *ReplicationManager
	glock             *sync.Mutex
	requestKeys       [][32]byte
	ackGlocks         []*sync.Mutex
	locks             []map[[16]byte]*Lock
	requests          []map[[32]byte][16]byte
	ackLocks          []map[[16]byte]*ReplicationAckLock
	freeAckLocks      []*ReplicationAckLock
	freeAckLocksIndex uint32
	freeAckLocksMax   uint32
	ackMaxGlocks      uint16
	ackCount          uint8
	closed            bool
}

func NewReplicationAckDB(manager *ReplicationManager) *ReplicationAckDB {
	ackMaxGlocks := uint16(Config.DBConcurrent)
	if ackMaxGlocks == 0 {
		ackMaxGlocks = uint16(runtime.NumCPU()) * 2
	}
	requestKeys := make([][32]byte, ackMaxGlocks)
	ackGlocks := make([]*sync.Mutex, ackMaxGlocks)
	locks := make([]map[[16]byte]*Lock, ackMaxGlocks)
	requests := make([]map[[32]byte][16]byte, ackMaxGlocks)
	ackLocks := make([]map[[16]byte]*ReplicationAckLock, ackMaxGlocks)
	for i := uint16(0); i < ackMaxGlocks; i++ {
		requestKeys[i] = [32]byte{}
		ackGlocks[i] = &sync.Mutex{}
		locks[i] = make(map[[16]byte]*Lock, REPLICATION_ACK_DB_INIT_SIZE)
		requests[i] = make(map[[32]byte][16]byte, REPLICATION_ACK_DB_INIT_SIZE)
		ackLocks[i] = make(map[[16]byte]*ReplicationAckLock, REPLICATION_ACK_DB_INIT_SIZE)
	}
	return &ReplicationAckDB{manager, &sync.Mutex{}, requestKeys, ackGlocks,
		locks, requests, ackLocks, make([]*ReplicationAckLock, REPLICATION_MAX_FREE_ACK_LOCK_QUEUE_SIZE*int(ackMaxGlocks)),
		0, uint32(REPLICATION_MAX_FREE_ACK_LOCK_QUEUE_SIZE * int(ackMaxGlocks)), ackMaxGlocks, 1, false}
}

func (self *ReplicationAckDB) Close() error {
	self.closed = true
	return nil
}

func (self *ReplicationAckDB) updateRequestKey(glockIndex uint16, lock *AofLock) [32]byte {
	requestKey := self.requestKeys[glockIndex]
	copy(requestKey[:], lock.LockKey[:])
	copy(requestKey[:][16:], lock.LockId[:])
	return requestKey
}

func (self *ReplicationAckDB) PushLock(glockIndex uint16, lock *AofLock) error {
	self.ackGlocks[glockIndex].Lock()
	if self.manager.slock.state != STATE_LEADER {
		if lock.lock == nil {
			self.ackGlocks[glockIndex].Unlock()
			return nil
		}

		self.ackGlocks[glockIndex].Unlock()
		lockManager := lock.lock.manager
		lockManager.lockDb.DoAckLock(lock.lock, false)
		return nil
	}

	requestKey := self.updateRequestKey(glockIndex, lock)
	if requestId, ok := self.requests[glockIndex][requestKey]; ok {
		if _, ok := self.locks[glockIndex][requestId]; ok {
			delete(self.locks[glockIndex], requestId)
		}
	}

	requestId := lock.GetRequestId()
	self.locks[glockIndex][requestId] = lock.lock
	self.requests[glockIndex][requestKey] = requestId
	lock.lock.ackCount = self.ackCount
	self.ackGlocks[glockIndex].Unlock()
	return nil
}

func (self *ReplicationAckDB) PushUnLock(glockIndex uint16, lock *AofLock) error {
	self.ackGlocks[glockIndex].Lock()
	requestKey := self.updateRequestKey(glockIndex, lock)
	if requestId, ok := self.requests[glockIndex][requestKey]; ok {
		delete(self.requests[glockIndex], requestKey)
		if lock, ok := self.locks[glockIndex][requestId]; ok {
			delete(self.locks[glockIndex], requestId)
			self.ackGlocks[glockIndex].Unlock()

			lockManager := lock.manager
			lockManager.lockDb.DoAckLock(lock, false)
			return nil
		}
	}
	self.ackGlocks[glockIndex].Unlock()
	return nil
}

func (self *ReplicationAckDB) Process(glockIndex uint16, aofLock *AofLock) error {
	requestId := aofLock.GetRequestId()
	self.ackGlocks[glockIndex].Lock()
	if lock, ok := self.locks[glockIndex][requestId]; ok {
		if aofLock.Result != 0 || lock.ackCount == 0xff {
			delete(self.locks[glockIndex], requestId)
			requestKey := self.updateRequestKey(glockIndex, aofLock)
			if _, ok := self.requests[glockIndex][requestKey]; ok {
				delete(self.requests[glockIndex], requestKey)
			}
			self.ackGlocks[glockIndex].Unlock()

			lockManager := lock.manager
			lockManager.lockDb.DoAckLock(lock, false)
			return nil
		}

		lock.ackCount--
		if lock.ackCount > 0 {
			self.ackGlocks[glockIndex].Unlock()
			return nil
		}

		delete(self.locks[glockIndex], requestId)
		requestKey := self.updateRequestKey(glockIndex, aofLock)
		if _, ok := self.requests[glockIndex][requestKey]; ok {
			delete(self.requests[glockIndex], requestKey)
		}
		self.ackGlocks[glockIndex].Unlock()

		lockManager := lock.manager
		lockManager.lockDb.DoAckLock(lock, true)
		return nil
	}

	self.ackGlocks[glockIndex].Unlock()
	return nil
}

func (self *ReplicationAckDB) ProcessAofed(glockIndex uint16, aofLock *AofLock) error {
	requestId := aofLock.GetRequestId()
	self.ackGlocks[glockIndex].Lock()
	if lock, ok := self.locks[glockIndex][requestId]; ok {
		if aofLock.Result != 0 || lock.ackCount == 0xff {
			delete(self.locks[glockIndex], requestId)
			requestKey := self.updateRequestKey(glockIndex, aofLock)
			if _, ok := self.requests[glockIndex][requestKey]; ok {
				delete(self.requests[glockIndex], requestKey)
			}
			self.ackGlocks[glockIndex].Unlock()

			lockManager := lock.manager
			lockManager.lockDb.DoAckLock(lock, false)
			return nil
		}

		lock.ackCount--
		if lock.ackCount > 0 {
			self.ackGlocks[glockIndex].Unlock()
			return nil
		}

		delete(self.locks[glockIndex], requestId)
		requestKey := self.updateRequestKey(glockIndex, aofLock)
		if _, ok := self.requests[glockIndex][requestKey]; ok {
			delete(self.requests[glockIndex], requestKey)
		}
		self.ackGlocks[glockIndex].Unlock()

		lockManager := lock.manager
		lockManager.lockDb.DoAckLock(lock, true)
		return nil
	}

	self.ackGlocks[glockIndex].Unlock()
	return nil
}

func (self *ReplicationAckDB) PushAckLock(glockIndex uint16, aofLock *AofLock) error {
	requestId := aofLock.GetRequestId()
	self.ackGlocks[glockIndex].Lock()
	if ackLock, ok := self.ackLocks[glockIndex][requestId]; !ok {
		self.glock.Lock()
		if self.freeAckLocksIndex > 0 {
			self.freeAckLocksIndex--
			ackLock = self.freeAckLocks[self.freeAckLocksIndex]
			self.glock.Unlock()
			ackLock.locked = false
			ackLock.aofed = false
		} else {
			self.glock.Unlock()
			ackLock = NewReplicationAckLock()
		}
		self.ackLocks[glockIndex][requestId] = ackLock
	}
	self.ackGlocks[glockIndex].Unlock()
	return nil
}

func (self *ReplicationAckDB) ProcessAcked(glockIndex uint16, command *protocol.LockCommand, result uint8, lcount uint16, lrcount uint8) error {
	self.ackGlocks[glockIndex].Lock()
	ackLock, ok := self.ackLocks[glockIndex][command.RequestId]
	if !ok {
		self.ackGlocks[glockIndex].Unlock()
		return nil
	}

	ackLock.lockResult.CommandType = command.CommandType
	ackLock.lockResult.RequestId = command.RequestId
	ackLock.lockResult.Result = result
	ackLock.lockResult.Flag = 0
	ackLock.lockResult.DbId = command.DbId
	ackLock.lockResult.LockId = command.LockId
	ackLock.lockResult.LockKey = command.LockKey
	ackLock.lockResult.Lcount = lcount
	ackLock.lockResult.Count = command.Count
	ackLock.lockResult.Lrcount = lrcount
	ackLock.lockResult.Rcount = command.Rcount
	ackLock.locked = true

	if !ackLock.aofed {
		self.ackGlocks[glockIndex].Unlock()
		return nil
	}

	delete(self.ackLocks[glockIndex], command.RequestId)
	self.ackGlocks[glockIndex].Unlock()
	if self.manager.clientChannel != nil {
		_ = self.manager.clientChannel.HandleAcked(ackLock)
	}
	self.glock.Lock()
	if self.freeAckLocksIndex < self.freeAckLocksMax {
		self.freeAckLocks[self.freeAckLocksIndex] = ackLock
		self.freeAckLocksIndex++
	}
	self.glock.Unlock()
	return nil
}

func (self *ReplicationAckDB) ProcessAckAofed(glockIndex uint16, aofLock *AofLock) error {
	requestId := aofLock.GetRequestId()
	self.ackGlocks[glockIndex].Lock()
	ackLock, ok := self.ackLocks[glockIndex][requestId]
	if !ok {
		self.glock.Lock()
		if self.freeAckLocksIndex > 0 {
			self.freeAckLocksIndex--
			ackLock = self.freeAckLocks[self.freeAckLocksIndex]
			self.glock.Unlock()
			ackLock.locked = false
		} else {
			self.glock.Unlock()
			ackLock = NewReplicationAckLock()
		}
		self.ackLocks[glockIndex][requestId] = ackLock
	}

	ackLock.aofResult = aofLock.Result
	ackLock.aofed = true

	if !ackLock.locked {
		self.ackGlocks[glockIndex].Unlock()
		return nil
	}

	delete(self.ackLocks[glockIndex], requestId)
	self.ackGlocks[glockIndex].Unlock()
	if self.manager.clientChannel != nil {
		_ = self.manager.clientChannel.HandleAcked(ackLock)
	}
	self.glock.Lock()
	if self.freeAckLocksIndex < self.freeAckLocksMax {
		self.freeAckLocks[self.freeAckLocksIndex] = ackLock
		self.freeAckLocksIndex++
	}
	self.glock.Unlock()
	return nil
}

func (self *ReplicationAckDB) SwitchToLeader() error {
	for i, ackLocks := range self.ackLocks {
		self.ackGlocks[i].Lock()
		for _, ackLock := range ackLocks {
			if self.manager.clientChannel != nil {
				ackLock.aofResult = protocol.RESULT_ERROR
				_ = self.manager.clientChannel.HandleAcked(ackLock)
			}

			self.glock.Lock()
			if self.freeAckLocksIndex < self.freeAckLocksMax {
				self.freeAckLocks[self.freeAckLocksIndex] = ackLock
				self.freeAckLocksIndex++
			}
			self.glock.Unlock()
		}
		self.ackLocks[i] = make(map[[16]byte]*ReplicationAckLock, REPLICATION_ACK_DB_INIT_SIZE)
		self.ackGlocks[i].Unlock()
	}
	return nil
}

func (self *ReplicationAckDB) SwitchToFollower() error {
	for i, locks := range self.locks {
		self.ackGlocks[i].Lock()
		for _, lock := range locks {
			lockManager := lock.manager
			lockManager.lockDb.DoAckLock(lock, false)
		}
		self.locks[i] = make(map[[16]byte]*Lock, REPLICATION_ACK_DB_INIT_SIZE)
		self.requests[i] = make(map[[32]byte][16]byte, REPLICATION_ACK_DB_INIT_SIZE)
		self.ackGlocks[i].Unlock()
	}
	return nil
}

func (self *ReplicationAckDB) FlushDB() error {
	for i := uint16(0); i < self.ackMaxGlocks; i++ {
		self.ackGlocks[i].Lock()
		for _, lock := range self.locks[i] {
			lockManager := lock.manager
			lockManager.lockDb.DoAckLock(lock, false)
		}

		for _, ackLock := range self.ackLocks[i] {
			if self.manager.clientChannel != nil {
				ackLock.aofResult = protocol.RESULT_ERROR
				_ = self.manager.clientChannel.HandleAcked(ackLock)
			}

			self.glock.Lock()
			if self.freeAckLocksIndex < self.freeAckLocksMax {
				self.freeAckLocks[self.freeAckLocksIndex] = ackLock
				self.freeAckLocksIndex++
			}
			self.glock.Unlock()
		}

		self.locks[i] = make(map[[16]byte]*Lock, REPLICATION_ACK_DB_INIT_SIZE)
		self.requests[i] = make(map[[32]byte][16]byte, REPLICATION_ACK_DB_INIT_SIZE)
		self.ackLocks[i] = make(map[[16]byte]*ReplicationAckLock, REPLICATION_ACK_DB_INIT_SIZE)
		self.ackGlocks[i].Unlock()
	}
	return nil
}

type ReplicationManager struct {
	slock               *SLock
	glock               *sync.Mutex
	bufferQueue         *ReplicationBufferQueue
	ackDbs              []*ReplicationAckDB
	clientChannel       *ReplicationClient
	serverChannels      []*ReplicationServer
	transparencyManager *TransparencyManager
	currentRequestId    [16]byte
	leaderAddress       string
	serverCount         uint32
	serverActiveCount   uint32
	serverFlushWaiter   chan bool
	initedWaters        []chan bool
	closed              bool
	isLeader            bool
}

func NewReplicationManager() *ReplicationManager {
	transparencyManager := NewTransparencyManager()
	manager := &ReplicationManager{nil, &sync.Mutex{}, nil, make([]*ReplicationAckDB, 256),
		nil, make([]*ReplicationServer, 0), transparencyManager, [16]byte{}, "", 0,
		0, nil, make([]chan bool, 0), false, true}
	manager.bufferQueue = NewReplicationBufferQueue(manager, uint64(Config.AofRingBufferSize), uint64(Config.AofRingBufferMaxSize))
	return manager
}

func (self *ReplicationManager) GetCallMethods() map[string]BinaryServerProtocolCallHandler {
	handlers := make(map[string]BinaryServerProtocolCallHandler, 2)
	handlers["SYNC"] = self.commandHandleSyncCommand
	return handlers
}

func (self *ReplicationManager) GetHandlers() map[string]TextServerProtocolCommandHandler {
	handlers := make(map[string]TextServerProtocolCommandHandler, 2)
	return handlers
}

func (self *ReplicationManager) Init(leaderAddress string) error {
	self.leaderAddress = leaderAddress
	self.slock.Log().Infof("Replication aof ring buffer init size %d", int(Config.AofRingBufferSize))
	if self.slock.state == STATE_LEADER {
		self.currentRequestId = self.slock.aof.GetCurrentAofID()
		self.slock.Log().Infof("Replication init leader %x", self.currentRequestId)
	} else {
		self.currentRequestId = [16]byte{}
		_ = self.transparencyManager.ChangeLeader(leaderAddress)
		self.slock.Log().Infof("Replication init follower %s %x", leaderAddress, self.currentRequestId)
	}
	return nil
}

func (self *ReplicationManager) Close() {
	self.glock.Lock()
	if self.closed {
		self.glock.Unlock()
		return
	}
	self.closed = true
	self.glock.Unlock()
	_ = self.bufferQueue.Close()

	if self.clientChannel != nil {
		clientChannel := self.clientChannel
		_ = clientChannel.Close()
		<-clientChannel.closedWaiter
		self.clientChannel = nil
	}

	_ = self.WakeupServerChannel()
	_ = self.WaitServerSynced()
	for _, channel := range self.serverChannels {
		_ = channel.Close()
		<-channel.closedWaiter
	}
	self.glock.Lock()
	self.serverChannels = self.serverChannels[:0]
	for _, waiter := range self.initedWaters {
		waiter <- false
	}
	self.initedWaters = self.initedWaters[:0]
	for i, db := range self.ackDbs {
		if db != nil {
			_ = db.Close()
			self.ackDbs[i] = nil
		}
	}
	self.glock.Unlock()
	_ = self.transparencyManager.Close()
	<-self.transparencyManager.closedWaiter
	self.slock.logger.Infof("Replication closed")
}

func (self *ReplicationManager) WaitServerSynced() error {
	self.glock.Lock()
	if atomic.CompareAndSwapUint32(&self.serverActiveCount, 0, 0) {
		self.glock.Unlock()
		return nil
	}

	serverFlushWaiter := make(chan bool, 1)
	go func() {
		select {
		case <-serverFlushWaiter:
			return
		case <-time.After(30 * time.Second):
			self.serverFlushWaiter = nil
			close(serverFlushWaiter)
		}
	}()
	self.serverFlushWaiter = serverFlushWaiter
	self.glock.Unlock()
	<-self.serverFlushWaiter
	return nil
}

func (self *ReplicationManager) commandHandleSyncCommand(server_protocol *BinaryServerProtocol, command *protocol.CallCommand) (*protocol.CallResultCommand, error) {
	if self.closed {
		return protocol.NewCallResultCommand(command, 0, "STATE_ERROR", nil), io.EOF
	}

	channel := NewReplicationServer(self, server_protocol)
	self.slock.logger.Infof("Replication server recv client %s sync", server_protocol.RemoteAddr().String())
	result, err := channel.handleInitSync(command)
	if err != nil {
		channel.closed = true
		close(channel.closedWaiter)
		if err != io.EOF {
			self.slock.logger.Errorf("Replication server handle client %s init sync error %s %v", server_protocol.RemoteAddr().String(), err)
		}
		return result, err
	}

	if result != nil {
		channel.closed = true
		close(channel.closedWaiter)
		self.slock.logger.Infof("Replication server handle client %s start sync fail", server_protocol.RemoteAddr().String())
		return result, nil
	}

	self.slock.logger.Infof("Replication server handle client %s start sync", server_protocol.RemoteAddr().String())
	_ = self.addServerChannel(channel)
	server_protocol.stream.streamType = STREAM_TYPE_AOF
	go func() {
		err := channel.SendProcess()
		if err != nil {
			if err != io.EOF && !self.closed {
				self.slock.logger.Errorf("Replication handle client %s sync error %v", server_protocol.RemoteAddr().String(), err)
			}
			_ = channel.Close()
		}
		if self.serverCount == 0 {
			self.bufferQueue.dupCurrentIndex = 0xffffffffffffffff
		}
	}()
	err = channel.RecvProcess()
	channel.closed = true
	_ = self.WakeupServerChannel()
	_ = self.removeServerChannel(channel)
	close(channel.closedWaiter)
	if err != nil {
		if err != io.EOF && !self.closed {
			self.slock.logger.Errorf("Replication handle client %s process error %v", server_protocol.RemoteAddr().String(), err)
		}
		return nil, io.EOF
	}
	self.slock.logger.Infof("Replication server handle client %s closed", server_protocol.RemoteAddr().String())
	return nil, io.EOF
}

func (self *ReplicationManager) addServerChannel(channel *ReplicationServer) error {
	self.glock.Lock()
	self.serverChannels = append(self.serverChannels, channel)
	self.serverCount = uint32(len(self.serverChannels))
	if self.serverCount == 1 {
		self.bufferQueue.dupCurrentIndex = self.bufferQueue.currentIndex
	}

	ackCount := len(self.serverChannels) + 1
	for _, db := range self.ackDbs {
		if db != nil {
			db.ackCount = uint8(ackCount)
		}
	}
	self.glock.Unlock()
	return nil
}

func (self *ReplicationManager) removeServerChannel(channel *ReplicationServer) error {
	self.glock.Lock()
	serverChannels := make([]*ReplicationServer, 0)
	for _, c := range self.serverChannels {
		if channel != c {
			serverChannels = append(serverChannels, c)
		}
	}
	self.serverChannels = serverChannels
	self.serverCount = uint32(len(serverChannels))
	if self.serverCount == 0 {
		self.bufferQueue.dupCurrentIndex = 0xffffffffffffffff
	}

	ackCount := len(self.serverChannels) + 1
	for _, db := range self.ackDbs {
		if db != nil {
			db.ackCount = uint8(ackCount)
		}
	}

	if atomic.CompareAndSwapUint32(&self.serverActiveCount, 0, 0) {
		if self.serverFlushWaiter != nil {
			close(self.serverFlushWaiter)
			self.serverFlushWaiter = nil
		}
	}
	self.glock.Unlock()
	return nil
}

func (self *ReplicationManager) StartSync() error {
	if self.leaderAddress == "" {
		return errors.New("slaveof is empty")
	}

	channel := NewReplicationClient(self)
	self.isLeader = false
	self.clientChannel = channel
	go channel.Run()
	self.slock.logger.Infof("Replication start sync")
	return nil
}

func (self *ReplicationManager) GetAckDB(dbId uint8) *ReplicationAckDB {
	return self.ackDbs[dbId]
}

func (self *ReplicationManager) GetOrNewAckDB(dbId uint8) *ReplicationAckDB {
	db := self.ackDbs[dbId]
	if db != nil {
		return db
	}

	self.glock.Lock()
	if self.ackDbs[dbId] == nil {
		self.ackDbs[dbId] = NewReplicationAckDB(self)
		self.ackDbs[dbId].ackCount = uint8(len(self.serverChannels) + 1)
	}
	self.glock.Unlock()
	return self.ackDbs[dbId]
}

func (self *ReplicationManager) PushLock(glockIndex uint16, lock *AofLock) error {
	if lock.CommandType == protocol.COMMAND_LOCK {
		if lock.AofFlag&AOF_FLAG_REQUIRE_ACKED != 0 && lock.lock != nil {
			db := self.GetOrNewAckDB(lock.DbId)
			err := db.PushLock(glockIndex, lock)
			if err != nil {
				return err
			}
		}
	}

	buf := lock.buf
	err := self.bufferQueue.Push(buf)
	if err != nil {
		return err
	}

	self.currentRequestId[0], self.currentRequestId[1], self.currentRequestId[2], self.currentRequestId[3], self.currentRequestId[4], self.currentRequestId[5], self.currentRequestId[6], self.currentRequestId[7],
		self.currentRequestId[8], self.currentRequestId[9], self.currentRequestId[10], self.currentRequestId[11], self.currentRequestId[12], self.currentRequestId[13], self.currentRequestId[14], self.currentRequestId[15] = buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9], buf[10],
		buf[11], buf[12], buf[13], buf[14], buf[15], buf[16], buf[17], buf[18]
	return nil
}

func (self *ReplicationManager) WakeupServerChannel() error {
	if self.bufferQueue.currentIndex%64 == 0 {
		var delayChannel *ReplicationServer = nil
		self.bufferQueue.dupCurrentIndex = 0xffffffffffffffff
		for _, channel := range self.serverChannels {
			if atomic.CompareAndSwapUint32(&channel.pulled, 1, 0) {
				channel.pulledWaiter <- true
			}

			if self.bufferQueue.currentIndex >= channel.bufferIndex {
				if self.bufferQueue.dupCurrentIndex > channel.bufferIndex {
					self.bufferQueue.dupCurrentIndex = channel.bufferIndex
					delayChannel = channel
				}
			} else {
				if self.bufferQueue.dupCurrentIndex == 0xffffffffffffffff || self.bufferQueue.dupCurrentIndex < channel.bufferIndex {
					self.bufferQueue.dupCurrentIndex = channel.bufferIndex
					delayChannel = channel
				}
			}
		}

		if delayChannel != nil {
			delayChannel.wakeupedBuffer = true
		}
		return nil
	}

	if atomic.CompareAndSwapUint32(&self.serverActiveCount, self.serverCount, self.serverCount) {
		return nil
	}
	for _, channel := range self.serverChannels {
		if atomic.CompareAndSwapUint32(&channel.pulled, 1, 1) {
			channel.pulledWaiter <- true
			atomic.AddUint32(&channel.pulled, 0xffffffff)
		}
	}
	return nil
}

func (self *ReplicationManager) WaitInitSynced(waiter chan bool) {
	self.glock.Lock()
	self.initedWaters = append(self.initedWaters, waiter)
	self.glock.Unlock()
}

func (self *ReplicationManager) wakeupInitSyncedWaiters() {
	self.glock.Lock()
	for _, waiter := range self.initedWaters {
		waiter <- false
	}
	self.initedWaters = self.initedWaters[:0]
	self.glock.Unlock()
}

func (self *ReplicationManager) clientSycnInited() {
	self.glock.Lock()
	self.slock.updateState(STATE_FOLLOWER)
	for _, waiter := range self.initedWaters {
		waiter <- true
	}
	self.initedWaters = self.initedWaters[:0]
	self.glock.Unlock()
}

func (self *ReplicationManager) SwitchToLeader() error {
	self.glock.Lock()
	if self.slock.state == STATE_CLOSE {
		self.glock.Unlock()
		return errors.New("state error")
	}

	self.slock.logger.Infof("Replication start change to leader")
	for _, db := range self.ackDbs {
		if db != nil {
			_ = db.SwitchToLeader()
		}
	}
	self.slock.updateState(STATE_LEADER)
	self.leaderAddress = ""
	self.glock.Unlock()

	if self.clientChannel != nil {
		clientChannel := self.clientChannel
		_ = clientChannel.Close()
		<-clientChannel.closedWaiter
		self.currentRequestId = clientChannel.currentRequestId
		self.clientChannel = nil
	}
	self.isLeader = true
	self.slock.logger.Infof("Replication finish change to leader")
	return nil
}

func (self *ReplicationManager) SwitchToFollower(address string) error {
	self.glock.Lock()
	if self.slock.state == STATE_CLOSE {
		self.glock.Unlock()
		return errors.New("state error")
	}

	if self.leaderAddress == address && !self.isLeader {
		self.glock.Unlock()
		return nil
	}

	self.slock.logger.Infof("Replication start change to follower, leader %s", address)
	self.leaderAddress = address
	if address == "" {
		self.slock.updateState(STATE_FOLLOWER)
	} else {
		self.slock.updateState(STATE_SYNC)
	}
	self.glock.Unlock()

	for _, db := range self.slock.dbs {
		if db != nil {
			for i := uint16(0); i < db.managerMaxGlocks; i++ {
				db.managerGlocks[i].Lock()
				db.managerGlocks[i].Unlock()
			}
		}
	}
	_ = self.slock.aof.WaitFlushAofChannel()
	_ = self.WakeupServerChannel()
	_ = self.WaitServerSynced()
	for _, channel := range self.serverChannels {
		_ = channel.Close()
		<-channel.closedWaiter
	}

	for _, db := range self.ackDbs {
		if db != nil {
			_ = db.SwitchToFollower()
		}
	}

	if self.clientChannel != nil {
		clientChannel := self.clientChannel
		_ = clientChannel.Close()
		<-clientChannel.closedWaiter
		self.currentRequestId = clientChannel.currentRequestId
		self.clientChannel = nil
	}

	if self.leaderAddress == "" {
		self.isLeader = false
		self.slock.logger.Infof("Replication finish change to follower, leader empty")
		return nil
	}

	err := self.StartSync()
	if err != nil {
		return err
	}
	self.slock.logger.Infof("Replication finish change to follower, leader %s", address)
	return nil
}

func (self *ReplicationManager) ChangeLeader(address string) error {
	self.glock.Lock()
	if self.slock.state != STATE_FOLLOWER {
		self.glock.Unlock()
		return errors.New("state error")
	}

	if self.leaderAddress == address {
		self.glock.Unlock()
		return nil
	}

	self.slock.logger.Infof("Replication follower start change current leader %s", address)
	self.leaderAddress = address
	if self.leaderAddress == "" {
		self.slock.updateState(STATE_FOLLOWER)
	} else {
		self.slock.updateState(STATE_SYNC)
	}
	self.glock.Unlock()

	if self.clientChannel != nil {
		clientChannel := self.clientChannel
		_ = clientChannel.Close()
		<-clientChannel.closedWaiter
		self.currentRequestId = clientChannel.currentRequestId
		self.clientChannel = nil
	}

	if self.leaderAddress == "" {
		self.isLeader = false
		self.slock.logger.Infof("Replication follower finish change current empty leader")
		return nil
	}

	err := self.StartSync()
	if err != nil {
		return err
	}
	self.slock.logger.Infof("Replication follower finish change current leader %s", address)
	return nil
}

func (self *ReplicationManager) FlushDB() error {
	_ = self.slock.aof.WaitFlushAofChannel()

	for _, db := range self.slock.dbs {
		if db != nil {
			_ = db.FlushDB()
		}
	}

	if self.slock.state != STATE_LEADER {
		for _, db := range self.ackDbs {
			if db != nil {
				_ = db.FlushDB()
			}
		}
	}

	self.bufferQueue.currentIndex = self.bufferQueue.segmentCount
	if self.serverCount > 0 {
		self.bufferQueue.dupCurrentIndex = self.bufferQueue.segmentCount
	} else {
		self.bufferQueue.dupCurrentIndex = 0xffffffffffffffff
	}
	self.slock.Log().Infof("Replication flush all DB")
	return nil
}

func (self *ReplicationManager) GetCurrentAofID() [16]byte {
	if !self.isLeader && self.clientChannel != nil {
		return self.clientChannel.currentRequestId
	}
	return self.currentRequestId
}
