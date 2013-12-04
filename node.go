package riakpbc

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"
)

type Node struct {
	addr         string
	tcpAddr      *net.TCPAddr
	conn         *net.TCPConn
	readTimeout  time.Duration
	writeTimeout time.Duration
	errorRate    *Decaying
	ok           bool
	oklock       *sync.Mutex
	sync.Mutex
}

// Returns a new Node.
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
		oklock:       &sync.Mutex{},
	}

	return node, nil
}

// Dial connects to a single riak node.
func (node *Node) Dial() (err error) {
	node.conn, err = net.DialTCP("tcp", nil, node.tcpAddr)
	if err != nil {
		return err
	}

	node.conn.SetKeepAlive(true)

	return nil
}

// ErrorRate safely returns the current Node's error rate.
func (node *Node) ErrorRate() float64 {
	return node.errorRate.Value()
}

// RecordError increments the current error value - see decaying.go
func (node *Node) RecordError(amount float64) {
	node.SetOk(false)
	node.errorRate.Add(amount)
}

func (node *Node) GetOk() bool {
	var out bool
	node.oklock.Lock()
	out = node.ok
	node.oklock.Unlock()
	return out
}

func (node *Node) SetOk(ok bool) {
	node.oklock.Lock()
	node.ok = ok
	node.oklock.Unlock()
}

func (node *Node) IsConnected() bool {
	return node.conn != nil
}

func (node *Node) ReqResp(reqstruct interface{}, structname string, raw bool) (response interface{}, err error) {
	node.Lock()
	defer node.Unlock()
	if node.IsConnected() != true {
		err = node.Dial()
		if err != nil {
			return nil, err
		}
	}
	if raw == true {
		err = node.rawRequest(reqstruct.([]byte), structname)
	} else {
		err = node.request(reqstruct, structname)
	}

	if err != nil {
		node.Close()
		return nil, err
	}

	response, err = node.response()
	if err != nil {
		node.Close()
		return nil, err
	}

	return
}

func (node *Node) ReqMultiResp(reqstruct interface{}, structname string) (response interface{}, err error) {
	response, err = node.ReqResp(reqstruct, structname, false)
	if err != nil {
		return nil, err
	}

	if structname == "RpbListKeysReq" {
		keys := response.(*RpbListKeysResp).GetKeys()
		done := response.(*RpbListKeysResp).GetDone()
		for done != true {
			response, err := node.response()
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
			response, err := node.response()
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

func (node *Node) Ping() bool {
	resp, err := node.ReqResp([]byte{}, "RpbPingReq", true)
	if err != nil {
		return false
	}
	if resp == nil || string(resp.([]byte)) != "Pong" {
		return false
	}
	return true
}

// Close the connection
func (node *Node) Close() {
	node.conn.Close()
	node.conn = nil
}

func (node *Node) read() (respraw []byte, err error) {
	node.conn.SetReadDeadline(time.Now().Add(node.readTimeout))

	buf := make([]byte, 4)
	var size int32

	// First 4 bytes are always size of message.
	n, err := io.ReadFull(node.conn, buf)
	if err != nil {
		node.RecordError(1.0)
		return nil, err
	}

	if n == 4 {
		sbuf := bytes.NewBuffer(buf)
		binary.Read(sbuf, binary.BigEndian, &size)
		data := make([]byte, size)
		// read rest of message
		m, err := io.ReadFull(node.conn, data)
		if err != nil {
			node.RecordError(1.0)
			return nil, err
		}
		if m == int(size) {
			return data, nil // return message
		}
	}

	node.RecordError(1.0)
	return nil, nil
}

func (node *Node) response() (response interface{}, err error) {
	rawresp, err := node.read()
	if err != nil {
		return nil, err
	}

	err = validateResponseHeader(rawresp)
	if err != nil {
		node.RecordError(1.0)
		return nil, err
	}

	response, err = unmarshalResponse(rawresp)
	if response == nil || err != nil {
		if err.Error() == "object not found" {
			return nil, err
		}
		return nil, err
	}

	return response, nil
}

func (node *Node) write(formattedRequest []byte) (err error) {
	node.conn.SetWriteDeadline(time.Now().Add(node.writeTimeout))

	_, err = node.conn.Write(formattedRequest)
	if err != nil {
		node.RecordError(1.0)
		return err
	}

	return nil
}

func (node *Node) request(reqstruct interface{}, structname string) (err error) {
	marshaledRequest, err := proto.Marshal(reqstruct.(proto.Message))
	if err != nil {
		return err
	}

	err = node.rawRequest(marshaledRequest, structname)
	if err != nil {
		return err
	}

	return
}

func (node *Node) rawRequest(marshaledRequest []byte, structname string) (err error) {
	formattedRequest, err := prependRequestHeader(structname, marshaledRequest)
	if err != nil {
		return err
	}

	err = node.write(formattedRequest)
	if err != nil {
		return err
	}
	return
}
