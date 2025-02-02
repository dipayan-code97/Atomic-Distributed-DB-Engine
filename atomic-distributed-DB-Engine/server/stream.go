package server

import (
	"io"
	"net"
	"sync/atomic"
	"time"
)

var clientId uint64 = 0

const (
	STREAM_TYPE_NORMAL  uint8 = 0
	STREAM_TYPE_AOF     uint8 = 1
	STREAM_TYPE_ARBITER uint8 = 2
)

type Stream struct {
	conn         net.Conn
	protocol     ServerProtocol
	startTime    *time.Time
	streamId     uint64
	streamType   uint8
	closed       bool
	closedWaiter chan bool
	nextStream   *Stream
	lastStream   *Stream
}

func NewStream(conn net.Conn) *Stream {
	now := time.Now()
	stream := &Stream{conn, nil, &now, atomic.AddUint64(&clientId, 1),
		STREAM_TYPE_NORMAL, false, make(chan bool, 1), nil, nil}
	return stream
}

func (self *Stream) ReadBytes(b []byte) (int, error) {
	if self.closed {
		return 0, io.EOF
	}

	cn := len(b)
	n, err := self.conn.Read(b)
	if err != nil {
		return n, err
	}

	if n < cn {
		for n < cn {
			nn, nerr := self.conn.Read(b[n:])
			if nerr != nil {
				return n + nn, nerr
			}
			n += nn
		}
	}
	return n, nil
}

func (self *Stream) Read(b []byte) (int, error) {
	if self.closed {
		return 0, io.EOF
	}

	return self.conn.Read(b)
}

func (self *Stream) WriteBytes(b []byte) error {
	if self.closed {
		return io.EOF
	}

	cn := len(b)
	n, err := self.conn.Write(b)
	if err != nil {
		return err
	}

	if n < cn {
		for n < cn {
			nn, nerr := self.conn.Write(b[n:])
			if nerr != nil {
				return nerr
			}
			n += nn
		}
	}
	return nil
}

func (self *Stream) Write(b []byte) (int, error) {
	if self.closed {
		return 0, io.EOF
	}

	return self.conn.Write(b)
}

func (self *Stream) Close() error {
	if self.closed {
		return nil
	}

	self.closed = true
	self.protocol = nil
	return self.conn.Close()
}

func (self *Stream) LocalAddr() net.Addr {
	return self.conn.LocalAddr()
}

func (self *Stream) RemoteAddr() net.Addr {
	return self.conn.RemoteAddr()
}

func (self *Stream) SetDeadline(t time.Time) error {
	return self.conn.SetDeadline(t)
}

func (self *Stream) SetReadDeadline(t time.Time) error {
	return self.conn.SetReadDeadline(t)
}

func (self *Stream) SetWriteDeadline(t time.Time) error {
	return self.conn.SetWriteDeadline(t)
}
