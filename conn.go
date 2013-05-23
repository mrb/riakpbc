// riakpbc is a Protocol Buffers based Riak client for Go
package riakpbc

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"
)

type Conn struct {
	mu           sync.Mutex
	conn         *net.TCPConn
	addr         string
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// Returns a new Conn connection
func New(addr string, readTimeout, writeTimeout time.Duration) (*Conn, error) {
	return &Conn{addr: addr, readTimeout: readTimeout, writeTimeout: writeTimeout}, nil
}

// Dial connects to a single riak server.
func (c *Conn) Dial() (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	tcpaddr, err := net.ResolveTCPAddr("tcp", c.addr)
	if err != nil {
		return err
	}

	c.conn, err = net.DialTCP("tcp", nil, tcpaddr)
	if err != nil {
		return err
	}

	return nil
}

// Close the connection
func (c *Conn) Close() {
	c.conn.Close()
}

// Write data to the connection
func (c *Conn) Write(formattedRequest []byte) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err = c.conn.Write(formattedRequest)
	if err != nil {
		return err
	}

	return nil
}

// Read data from the connection
func (c *Conn) Read() (respraw []byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	buf := make([]byte, 4)
	var size int32
	// First 4 bytes are always size of message.
	n, err := io.ReadFull(c.conn, buf)
	if err != nil {
		return nil, err
	}
	if n == 4 {
		sbuf := bytes.NewBuffer(buf)
		binary.Read(sbuf, binary.BigEndian, &size)
		data := make([]byte, size)
		// read rest of message
		m, err := io.ReadFull(c.conn, data)
		if err != nil {
			return nil, err
		}
		if m == int(size) {
			return data, nil // return message
		}
	}

	return nil, nil
}
