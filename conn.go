package riakpbc

import (
	"net"
)

type Conn struct {
	addr string
	conn net.Conn
}

// Dial connects to a single riak server.
func Dial(addr string) (*Conn, error) {
	var c Conn
	var err error
	c.addr = addr
	c.conn, err = net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &c, nil
}

func (c *Conn) Close() {
	c.conn.Close()
}

func (c *Conn) Write(formattedRequest []byte) (err error) {
	_, err = c.conn.Write(formattedRequest)
	return err
}

func (c *Conn) Read() (respraw []byte, err error) {
	respraw = make([]byte, 512)

	c.conn.Read(respraw)

	_ = respraw[3]

	return respraw, nil
}
