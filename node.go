package riakpbc

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type Node struct {
	coder        *Coder
	addr         string
	tcpAddr      *net.TCPAddr
	conn         *net.TCPConn
	readTimeout  time.Duration
	writeTimeout time.Duration
	errorRate    *Decaying
	opts         interface{} // potential Rpb...Req opts
	okLock       *sync.Mutex
	ok           bool
}

// Returns a new self.
func NewNode(addr string, readTimeout, writeTimeout time.Duration) (*Node, error) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	node := &Node{
		addr:         addr,
		tcpAddr:      tcpaddr,
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
		errorRate:    NewDecaying(),
		ok:           true,
		okLock:       &sync.Mutex{},
	}

	return node, nil
}

// Dial connects to a single riak node.
func (self *Node) Dial() (err error) {
	self.conn, err = net.DialTCP("tcp", nil, self.tcpAddr)
	if err != nil {
		return err
	}

	self.conn.SetKeepAlive(true)

	return nil
}

// ErrorRate safely returns the current Node's error rate
func (self *Node) ErrorRate() float64 {
	return self.errorRate.Value()
}

// RecordErrror increments the current error value - see decaying.go
func (self *Node) RecordError(amount float64) {
	self.SetOk(false)
	self.errorRate.Add(amount)
}

// Opts returns the set options, and resets them internally to nil.
func (self *Node) Opts() interface{} {
	opts := self.opts
	self.opts = nil
	return opts
}

func (self *Node) Ok() bool {
	self.okLock.Lock()
	ok := self.ok
	self.okLock.Unlock()
	return ok
}

func (self *Node) SetOk(ok bool) {
	self.okLock.Lock()
	self.ok = ok
	self.okLock.Unlock()
}

// SetOpts allows Rpb...Req options to be set for the currently selected self.
func (self *Node) SetOpts(opts interface{}) {
	self.opts = opts
}

func (self *Node) ReqResp(reqstruct interface{}, structname string, raw bool) (response interface{}, err error) {
	if raw == true {
		err = self.rawRequest(reqstruct.([]byte), structname)
	} else {
		err = self.request(reqstruct, structname)
	}

	if err != nil {
		return nil, err
	}

	response, err = self.response()
	if err != nil {
		return nil, err
	}

	return
}

func (self *Node) ReqMultiResp(reqstruct interface{}, structname string) (response interface{}, err error) {
	response, err = self.ReqResp(reqstruct, structname, false)
	if err != nil {
		return nil, err
	}

	if structname == "RpbListKeysReq" {
		keys := response.(*RpbListKeysResp).GetKeys()
		done := response.(*RpbListKeysResp).GetDone()
		for done != true {
			response, err := self.response()
			if err != nil {
				return nil, err
			}
			keys = append(keys, response.(*RpbListKeysResp).GetKeys()...)
			done = response.(*RpbListKeysResp).GetDone()
		}
		return keys, nil
	} else if structname == "RpbMapRedReq" {
		mapResponse := response.(*RpbMapRedResp).GetResponse()
		done := response.(*RpbMapRedResp).GetDone()
		for done != true {
			response, err := self.response()
			if err != nil {
				return nil, err
			}
			mapResponse = append(mapResponse, response.(*RpbMapRedResp).GetResponse()...)
			done = response.(*RpbMapRedResp).GetDone()
		}
		return mapResponse, nil
	}
	return nil, nil
}

func (self *Node) DoPing() bool {
	log.Print("Pinging ", self)
	resp, err := self.ReqResp([]byte{}, "RpbPingReq", true)
	if resp == nil || string(resp.([]byte)) != "Pong" || err != nil {
		return false
	}
	return true
}

// Close the connection
func (self *Node) Close() {
	self.conn.Close()
	self.conn = nil
}

func (self *Node) write(formattedRequest []byte) (err error) {
	self.conn.SetWriteDeadline(time.Now().Add(self.readTimeout))
	_, err = self.conn.Write(formattedRequest)
	if err != nil {
		return err
	}

	return nil
}

func (self *Node) read() (respraw []byte, err error) {
	self.conn.SetWriteDeadline(time.Now().Add(self.readTimeout))

	buf := make([]byte, 4)
	var size int32
	// First 4 bytes are always size of message.
	n, err := io.ReadFull(self.conn, buf)

	if err != nil {
		return nil, err
	}
	if n == 4 {
		sbuf := bytes.NewBuffer(buf)
		binary.Read(sbuf, binary.BigEndian, &size)
		data := make([]byte, size)
		// read rest of message
		m, err := io.ReadFull(self.conn, data)
		if err != nil {
			self.RecordError(1.0)
			return nil, err
		}
		if m == int(size) {
			return data, nil // return message
		}
	}
	return nil, nil
}

func (self *Node) response() (response interface{}, err error) {
	rawresp, err := self.read()
	if err != nil {
		self.RecordError(1.0)
		return nil, err
	}

	err = validateResponseHeader(rawresp)
	if err != nil {
		self.RecordError(1.0)
		return nil, err
	}

	response, err = unmarshalResponse(rawresp)
	if err != nil || response == nil {
		self.RecordError(1.0)
		return nil, err
	}

	return response, nil
}

func (self *Node) request(reqstruct interface{}, structname string) (err error) {
	marshaledRequest, err := proto.Marshal(reqstruct.(proto.Message))
	if err != nil {
		self.RecordError(1.0)
		return err
	}

	err = self.rawRequest(marshaledRequest, structname)
	if err != nil {
		self.RecordError(1.0)
		return err
	}

	return
}

func (self *Node) rawRequest(marshaledRequest []byte, structname string) (err error) {
	formattedRequest, err := prependRequestHeader(structname, marshaledRequest)
	if err != nil {
		self.RecordError(1.0)
		return err
	}

	err = self.write(formattedRequest)
	if err != nil {
		self.RecordError(1.0)
		return err
	}
	return
}
