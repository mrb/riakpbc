package riakpbc

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"time"
)

type Node struct {
	addr         string
	conn         *net.TCPConn
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// Returns a new Node.
func NewNode(addr string, readTimeout, writeTimeout time.Duration) *Node {
	return &Node{
		addr:         addr,
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
}

// Dial connects to a single riak node.
func (node *Node) Dial() (err error) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", node.addr)
	if err != nil {
		return err
	}

	node.conn, err = net.DialTCP("tcp", nil, tcpaddr)
	if err != nil {
		return err
	}

	node.conn.SetKeepAlive(true)
	node.conn.SetReadDeadline(time.Now().Add(node.readTimeout))
	node.conn.SetWriteDeadline(time.Now().Add(node.readTimeout))

	return nil
}

// Close the connection
func (node *Node) Close() {
	node.conn.Close()
}

// Write data to the connection
func (node *Node) Write(formattedRequest []byte) (err error) {
	_, err = node.conn.Write(formattedRequest)
	if err != nil {
		return err
	}

	return nil
}

// Read data from the connection
func (node *Node) Read() (respraw []byte, err error) {
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
			return nil, err
		}
		if m == int(size) {
			return data, nil // return message
		}
	}

	return nil, nil
}
