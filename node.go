package riakpbc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"time"
)

type Node struct {
	addr         string
	tcpAddr      *net.TCPAddr
	conn         *net.TCPConn
	readTimeout  time.Duration
	writeTimeout time.Duration
	errorRate    *Decaying
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

func (node *Node) TestConn() error {
	_, err := bufio.NewReader(node.conn).Peek(1)
	return err
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
		node.errorRate.Add(1.0)
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
		node.errorRate.Add(1.0)
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
