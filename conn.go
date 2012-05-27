package riakpbc

import (
	"net"
	"time"
)

type Conn struct {
	addr string
	conn *net.TCPConn
}

// Dial connects to a single riak server.
func Dial(addr string) (*Conn, error) {
	var c Conn
	var err error

	c.addr = addr

	tcpaddr, err := net.ResolveTCPAddr("tcp", c.addr)
	if err != nil {
		return nil, err
	}

	c.conn, err = net.DialTCP("tcp", nil, tcpaddr)

	if err != nil {
		return nil, err
	}

	return &c, nil
}

func (c *Conn) Close() {
	c.conn.Close()
}

func (c *Conn) Write(formattedRequest []byte) (err error) {
	timeoutime := time.Now().Add(time.Duration(1e9))
	c.conn.SetWriteDeadline(timeoutime)

	_, err = c.conn.Write(formattedRequest)

	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		err = ErrWriteTimeout
		return err
	}

	return err
}

func (c *Conn) Read() (respraw []byte, err error) {
	respraw = make([]byte, 512)

	timeoutime := time.Now().Add(time.Duration(1e9))
	c.conn.SetReadDeadline(timeoutime)

	_, err = c.conn.Read(respraw)

	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		err = ErrReadTimeout
		return nil, err
	}

	_ = respraw[3]

	return respraw, nil
}
