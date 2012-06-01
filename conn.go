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

func (c *Conn) Write(formattedRequest []byte) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	timeoutime := time.Now().Add(time.Duration(c.writeTimeout))
	c.conn.SetWriteDeadline(timeoutime)

	_, err = c.conn.Write(formattedRequest)

	if err != nil {
		if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
			err = ErrWriteTimeout
			return err
		}

		return err
	}

	return nil
}

func (c *Conn) Read() (respraw []byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	respraw = make([]byte, 512)

	timeoutime := time.Now().Add(time.Duration(c.readTimeout))
	c.conn.SetReadDeadline(timeoutime)

	_, err = c.conn.Read(respraw)

	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		err = ErrReadTimeout
		return nil, err
	}

	_ = respraw[3]

	return respraw, nil
}
