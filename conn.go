package riakpbc

import (
	"net"
	"time"
)

type Conn struct {
	addr string
	conn *net.TCPConn
  readTimeout *int
  writeTimeout *int
}

// Returns a new Conn connection
func New () (c *Conn, err error){
  return err
}

// Dial connects to a single riak server.
func (c *Conn) Dial () (err error) {

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

// Close the connection
func (c *Conn) Close() {
	c.conn.Close()
}

func (c *Conn) Write(formattedRequest []byte) (err error) {
	timeoutime := time.Now().Add(time.Duration(c.writeTimeout))
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
