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
	addr           string
	tcpAddr        *net.TCPAddr
	conn           *net.TCPConn
	readTimeout    time.Duration
	writeTimeout   time.Duration
	errorRate      *Decaying
	errorRateMutex *sync.Mutex
	sync.Mutex
}

// Returns a new Node.
func NewNode(addr string, readTimeout, writeTimeout time.Duration) (*Node, error) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	node := &Node{
		addr:           addr,
		tcpAddr:        tcpaddr,
		readTimeout:    readTimeout,
		writeTimeout:   writeTimeout,
		errorRate:      NewDecaying(),
		errorRateMutex: &sync.Mutex{},
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

func (node *Node) ErrorRate() float64 {
	node.errorRateMutex.Lock()
	rate := node.errorRate.Value()
	node.errorRateMutex.Unlock()
	return rate
}

func (node *Node) RecordError(amount float64) {
	node.errorRateMutex.Lock()
	node.errorRate.Add(amount)
	node.errorRateMutex.Unlock()
}

// Close the connection
func (node *Node) Close() {
	node.conn.Close()
}

// Write data to the connection
func (node *Node) Write(formattedRequest []byte) (err error) {
	node.conn.SetWriteDeadline(time.Now().Add(node.readTimeout))
	_, err = node.conn.Write(formattedRequest)
	if err != nil {
		return err
	}

	return nil
}

// Read data from the connection
func (node *Node) Read() (respraw []byte, err error) {
	node.conn.SetWriteDeadline(time.Now().Add(node.readTimeout))

	buf := make([]byte, 4)
	var size int32
	// First 4 bytes are always size of message.
	n, err := io.ReadFull(node.conn, buf)

	if err != nil {
		return nil, err
	}
	if n == 4 {
		sbuf := bytes.NewBuffer(buf)
		binary.Read(sbuf, binary.BigEndian, &size)
		data := make([]byte, size)
		// read rest of message
		m, err := io.ReadFull(node.conn, data)
		if err != nil {
			node.errorRate.Add(1.0)
			return nil, err
		}
		if m == int(size) {
			return data, nil // return message
		}
	}
	return nil, nil
}

func (node *Node) Response() (response interface{}, err error) {
	rawresp, err := node.Read()

	if err != nil {
		node.RecordError(1.0)
		return nil, err
	}

	err = validateResponseHeader(rawresp)
	if err != nil {
		node.RecordError(1.0)
		return nil, err
	}

	response, err = unmarshalResponse(rawresp)
	if err != nil || response == nil {
		node.RecordError(1.0)
		return nil, err
	}

	return response, nil
}

func (node *Node) Request(reqstruct interface{}, structname string) (err error) {
	marshaledRequest, err := proto.Marshal(reqstruct.(proto.Message))

	if err != nil {
		node.RecordError(1.0)
		return err
	}

	err = node.RawRequest(marshaledRequest, structname)
	if err != nil {
		node.RecordError(1.0)
		return err
	}

	return
}

func (node *Node) RawRequest(marshaledRequest []byte, structname string) (err error) {
	formattedRequest, err := prependRequestHeader(structname, marshaledRequest)
	if err != nil {
		node.RecordError(1.0)
		return err
	}

	err = node.Write(formattedRequest)
	if err != nil {
		node.RecordError(1.0)
		return err
	}
	return
}
