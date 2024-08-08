package godev

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/snower/slock/protocol"
	"net"
	"strconv"
	"strings"
	"sync"
)

type ClientProtocol interface {
	close() error
	read() (protocol.CommandDecode, error)
	write(protocol.CommandEncode) error
	readCommand() (protocol.CommandDecode, error)
	writeCommand(protocol.CommandEncode) error
	getStream() *Stream
	remoteAddress() net.Addr
	localAddress() net.Addr
}

type BinaryClientProtocol struct {
	stream *Stream
	rglock *sync.Mutex
	wglock  *sync.Mutex
	rbuffer []byte
	wbuffer []byte
}


func BuildNewBinaryClientProtocol(stream *Stream) *BinaryClientProtocol {
	return &BinaryClientProtocol{stream, &sync.Mutex{}, &sync.Mutex{},
								 make([]byte, 64), make([]byte, 64)}
}

func (selfRef *BinaryClientProtocol) writerBuffer() []byte {
	return selfRef.wbuffer
}

func (selfRef *BinaryClientProtocol) setWriterBuffer(wbuffer []byte) {
	selfRef.wbuffer = wbuffer
}

func (selfRef *BinaryClientProtocol) readerBuffer() []byte {
	return selfRef.rbuffer
}

func (selfRef *BinaryClientProtocol) setReaderBuffer(rbuffer []byte) {
	selfRef.rbuffer = rbuffer
}

func (selfRef *BinaryClientProtocol) waitGroupLock() *sync.Mutex {
	return selfRef.wglock
}

func (selfRef *BinaryClientProtocol) setWaitGroupLock(waitGroupLock *sync.Mutex) {
	selfRef.wglock = waitGroupLock
}

func (selfRef *BinaryClientProtocol) Rglock() *sync.Mutex {
	return selfRef.rglock
}

func (selfRef *BinaryClientProtocol) SetRglock(rglock *sync.Mutex) {
	selfRef.rglock = rglock
}

func (selfRef *BinaryClientProtocol) Stream() *Stream {
	return selfRef.stream
}

func (selfRef *BinaryClientProtocol) SetStream(stream *Stream) {
	selfRef.stream = stream
}

func (selfRef *BinaryClientProtocol) close() error {
	return selfRef.stream.Close()
}

func (selfRef *BinaryClientProtocol) read() (protocol.CommandDecode, error) {
	defer selfRef.rglock.Unlock()
	selfRef.rglock.Lock()

	n, err := selfRef.stream.ReadBytes(selfRef.rbuffer)
	if err != nil {
		return nil, err
	}

	if n != 64 {
		return nil, errors.New("command data too short")
	}

	if uint8(selfRef.rbuffer[0]) != protocol.MAGIC {
		return nil, errors.New("unknown magic")
	}

	if uint8(selfRef.rbuffer[1]) != protocol.VERSION {
		return nil, errors.New("unknown version")
	}

	switch uint8(selfRef.rbuffer[2]) {
	case protocol.COMMAND_LOCK:
		command := protocol.LockResultCommand{}
		err := command.Decode(selfRef.rbuffer)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_UNLOCK:
		command := protocol.LockResultCommand{}
		err := command.Decode(selfRef.rbuffer)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.CommandState:
		command := protocol.StateResultCommand{}
		err := command.Decode(selfRef.rbuffer)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_INIT:
		command := protocol.InitResultCommand{}
		err := command.Decode(selfRef.rbuffer)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_ADMIN:
		command := protocol.AdminResultCommand{}
		err := command.Decode(selfRef.rbuffer)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_PING:
		command := protocol.PingResultCommand{}
		err := command.Decode(selfRef.rbuffer)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_QUIT:
		command := protocol.QuitResultCommand{}
		err := command.Decode(selfRef.rbuffer)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_CALL:
		command := protocol.CallResultCommand{}
		err := command.Decode(selfRef.rbuffer)
		if err != nil {
			return nil, err
		}
		command.Data = make([]byte, command.ContentLen)
		if command.ContentLen > 0 {
			_, err := selfRef.stream.ReadBytes(command.Data)
			if err != nil {
				return nil, err
			}
		}
		return &command, nil
	case protocol.COMMAND_WILL_LOCK:
		command := protocol.LockResultCommand{}
		err := command.Decode(selfRef.rbuffer)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_WILL_UNLOCK:
		command := protocol.LockResultCommand{}
		err := command.Decode(selfRef.rbuffer)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_LEADER:
		command := protocol.LeaderResultCommand{}
		err := command.Decode(selfRef.rbuffer)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_SUBSCRIBE:
		command := protocol.SubscribeResultCommand{}
		err := command.Decode(selfRef.rbuffer)
		if err != nil {
			return nil, err
		}
		return &command, nil
	case protocol.COMMAND_PUBLISH:
		command := protocol.LockResultCommand{}
		err := command.Decode(selfRef.rbuffer)
		if err != nil {
			return nil, err
		}
		return &command, nil
	default:
		return nil, errors.New("unknown command")
	}
}

func (selfRef *BinaryClientProtocol) write(command protocol.CommandEncode) error {
	selfRef.wglock.Lock()
	wbuffer := selfRef.wbuffer
	err := command.Encode(wbuffer)
	if err != nil {
		selfRef.wglock.Unlock()
		return err
	}

	err = selfRef.stream.WriteBytes(wbuffer)
	if err != nil {
		selfRef.wglock.Unlock()
		return err
	}

	switch command.(type) {
	case *protocol.CallCommand:
		callCommand := command.(*protocol.CallCommand)
		if callCommand.ContentLen > 0 {
			err = selfRef.stream.WriteBytes(callCommand.Data)
		}
	}
	selfRef.wglock.Unlock()
	return err
}

func (selfRef *BinaryClientProtocol) readCommand() (protocol.CommandDecode, error) {
	return selfRef.read()
}

func (selfRef *BinaryClientProtocol) writeCommand(command protocol.CommandEncode) error {
	return selfRef.write(command)
}

func (selfRef *BinaryClientProtocol) getStream() *Stream {
	return selfRef.stream
}

func (selfRef *BinaryClientProtocol) remoteAddress() net.Addr {
	return selfRef.stream.RemoteAddr()
}

func (selfRef *BinaryClientProtocol) localAddress() net.Addr {
	return selfRef.stream.LocalAddr()
}

type TextClientProtocol struct {
	stream *Stream
	rglock *sync.Mutex
	wglock *sync.Mutex
	parser *protocol.TextParser
}

func NewTextClientProtocol(stream *Stream) *TextClientProtocol {
	parser := protocol.NewTextParser(make([]byte, 1024), make([]byte, 1024))
	clientProtocol := &TextClientProtocol{stream, &sync.Mutex{}, &sync.Mutex{}, parser}
	return clientProtocol
}

func (selfRef *TextClientProtocol) close() error {
	return selfRef.stream.Close()
}

func (selfRef *TextClientProtocol) getParser() *protocol.TextParser {
	return selfRef.parser
}

func (selfRef *TextClientProtocol) argsToLockComandResultParseId(argId string, lockId *[16]byte) {
	argLen := len(argId)
	if argLen == 16 {
		lockId[0], lockId[1], lockId[2], lockId[3], lockId[4], lockId[5], lockId[6], lockId[7],
			lockId[8], lockId[9], lockId[10], lockId[11], lockId[12], lockId[13], lockId[14], lockId[15] =
			byte(argId[0]), byte(argId[1]), byte(argId[2]), byte(argId[3]), byte(argId[4]), byte(argId[5]), byte(argId[6]),
			byte(argId[7]), byte(argId[8]), byte(argId[9]), byte(argId[10]), byte(argId[11]), byte(argId[12]), byte(argId[13]),
			byte(argId[14]), byte(argId[15])
	} else if argLen > 16 {
		if argLen == 32 {
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
		argIndex := 16 - argLen
		for indexer := 0; indexer < 16; indexer++ {
			if indexer < argIndex {
				lockId[indexer] = 0
			} else {
				lockId[indexer] = argId[indexer-argIndex]
			}
		}
	}
}

func (selfRef *TextClientProtocol) argsToLockComandResult(args []string) (*protocol.LockResultCommand, error) {
	if len(args) < 2 || len(args)%2 != 0 {
		return nil, errors.New("Response Parse Len error")
	}

	lockCommandResult := protocol.LockResultCommand{}
	lockCommandResult.Magic = protocol.MAGIC
	lockCommandResult.Version = protocol.VERSION
	result, err := strconv.Atoi(args[0])
	if err != nil {
		return nil, errors.New("Response Parse Result error")
	}
	lockCommandResult.Result = uint8(result)

	for index := 2; index < len(args); index += 2 {
		switch strings.ToUpper(args[index]) {
		case "LOCK_ID":
			selfRef.argsToLockComandResultParseId(args[index+1], &lockCommandResult.LockId)
		case "LCOUNT":
			lcount, err := strconv.Atoi(args[index+1])
			if err != nil {
				return nil, errors.New("Response Parse LCOUNT error")
			}
			lockCommandResult.Lcount = uint16(lcount)
		case "COUNT":
			count, err := strconv.Atoi(args[index+1])
			if err != nil {
				return nil, errors.New("Response Parse COUNT error")
			}
			lockCommandResult.Count = uint16(count)
		case "LRCOUNT":
			lrcount, err := strconv.Atoi(args[index+1])
			if err != nil {
				return nil, errors.New("Response Parse LRCOUNT error")
			}
			lockCommandResult.Lrcount = uint8(lrcount)
		case "RCOUNT":
			rcount, err := strconv.Atoi(args[index+1])
			if err != nil {
				return nil, errors.New("Response Parse RCOUNT error")
			}
			lockCommandResult.Rcount = uint8(rcount)
		}
	}
	return &lockCommandResult, nil
}

func (selfRef *TextClientProtocol) read() (protocol.CommandDecode, error) {
	defer selfRef.rglock.Unlock()
	selfRef.rglock.Lock()

	rbuffer := selfRef.parser.GetReadBuf()
	for {
		if selfRef.parser.IsBufferEnd() {
			n, err := selfRef.stream.Read(rbuffer)
			if err != nil {
				return nil, err
			}

			selfRef.parser.BufferUpdate(n)
		}

		err := selfRef.parser.ParseResponse()
		if err != nil {
			return nil, err
		}

		if selfRef.parser.IsParseFinish() {
			command, err := selfRef.parser.GetResponseCommand()
			selfRef.parser.Reset()
			return command, err
		}
	}
}

func (selfRef *TextClientProtocol) write(result protocol.CommandEncode) error {
	switch result.(type) {
	case *protocol.LockResultCommand:
		return selfRef.writeCommand(result)
	case *protocol.TextRequestCommand:
		selfRef.wglock.Lock()
		err := selfRef.stream.WriteBytes(selfRef.parser.BuildRequest(result.(*protocol.TextRequestCommand).Args))
		selfRef.wglock.Unlock()
		return err
	}
	return errors.New("unknown command")
}

func (selfRef *TextClientProtocol) readCommand() (protocol.CommandDecode, error) {
	command, err := selfRef.read()
	if err != nil {
		return nil, err
	}

	textClientCommand := command.(*protocol.TextResponseCommand)
	if textClientCommand.ErrorType != "" {
		if textClientCommand.Message != "" {
			return nil, errors.New(textClientCommand.ErrorType + " " + textClientCommand.Message)
		}
		return nil, errors.New("unknown result")
	}

	lockCommandResult, err := selfRef.argsToLockComandResult(textClientCommand.Results)
	return lockCommandResult, err
}

func (selfRef *TextClientProtocol) writeCommand(result protocol.CommandEncode) error {
	defer selfRef.wglock.Unlock()
	selfRef.wglock.Lock()

	lockCommandResult, ok := result.(*protocol.LockResultCommand)
	if !ok {
		return errors.New("unknown result")
	}

	bufferIndex := 0
	tr := ""

	writeBuffer := selfRef.parser.GetWriteBuf()
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte("*12\r\n"))

	tr = fmt.Sprintf("%d", lockCommandResult.Result)
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte(fmt.Sprintf("$%d\r\n", len(tr))))
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte(tr))

	tr = protocol.ERROR_MSG[lockCommandResult.Result]
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte(tr))

	bufferIndex += copy(writeBuffer[bufferIndex:], []byte("\r\n$7\r\nLOCK_ID\r\n$32\r\n"))
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte(fmt.Sprintf("%x", lockCommandResult.LockId)))
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte("\r\n$6\r\nLCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Lcount)
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte(tr))

	bufferIndex += copy(writeBuffer[bufferIndex:], []byte("\r\n$5\r\nCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Count)
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte(tr))

	bufferIndex += copy(writeBuffer[bufferIndex:], []byte("\r\n$7\r\nLRCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Lrcount)
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte(tr))

	bufferIndex += copy(writeBuffer[bufferIndex:], []byte("\r\n$6\r\nRCOUNT"))

	tr = fmt.Sprintf("%d", lockCommandResult.Rcount)
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte(fmt.Sprintf("\r\n$%d\r\n", len(tr))))
	bufferIndex += copy(writeBuffer[bufferIndex:], []byte(tr))

	bufferIndex += copy(writeBuffer[bufferIndex:], []byte("\r\n"))

	return selfRef.stream.WriteBytes(writeBuffer[:bufferIndex])
}

func (selfRef *TextClientProtocol) getStream() *Stream {
	return selfRef.stream
}

func (selfRef *TextClientProtocol) remoteAddress() net.Addr {
	return selfRef.stream.RemoteAddr()
}

func (selfRef *TextClientProtocol) localAddress() net.Addr {
	return selfRef.stream.LocalAddr()
}
