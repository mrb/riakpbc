// riakpbc is a Protocol Buffers based Riak client for Go
package riakpbc

import (
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
func New(addr string, readTimeout time.Duration, writeTimeout time.Duration) (*Conn, error) {
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
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			err = ErrWriteTimeout
		}

		return err
	}

	return nil
}

// Read data from the connection
func (c *Conn) Read() (respraw []byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	respraw = make([]byte, 512)

	_, err = c.conn.Read(respraw)

	if err != nil {
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			err = ErrReadTimeout
		}
		return nil, err
	}

	return respraw, nil
}
