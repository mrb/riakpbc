package riakpbc

import (
	"log"
	"sync"
	"time"
)

type Client struct {
	cluster       []string
	pool          *Pool
	Coder         *Coder // Coder for (un)marshalling data
	optsMu        *sync.Mutex
	opts          interface{} // potential Rpb...Req opts
	logging       bool
	pingFrequency int
}

// NewClient accepts a slice of node address strings and returns a Client object.
func NewClient(cluster []string) *Client {
	return &Client{
		cluster:       cluster,
		pool:          NewPool(cluster),
		optsMu:        &sync.Mutex{},
		logging:       false,
		pingFrequency: 500,
	}
}

// Dial connects all nodes in the pool to their addresses via TCP.
//
// Illegally addressed nodes will be rejected here.
func (c *Client) Dial() error {
	for k, node := range c.pool.nodes {
		err := node.Dial()
		if err != nil {
			if c.LoggingEnabled() {
				log.Print("[POOL] Error: ", err)
			}
			c.pool.DeleteNode(k)
		}
	}

	if c.LoggingEnabled() {
		log.Print("[POOL] Riak Dialed. Connected to ", c.pool.Size(), " Riak nodes.")
	}

	if c.pool.Size() < 1 {
		return ErrZeroNodes
	}

	go c.BackgroundNodePing()

	return nil
}

func (c *Client) BackgroundNodePing() {
	for {
		time.Sleep(time.Duration(c.pingFrequency) * time.Millisecond)
		c.pool.Ping()
	}
}

// Opts returns the set options, and resets them internally to nil.
func (c *Client) Opts() interface{} {
	c.optsMu.Lock()
	opts := c.opts
	c.opts = nil
	c.optsMu.Unlock()
	return opts
}

// Current gets the current Node object from the Pool.
func (c *Client) Current() *Node {
	return c.pool.Current()
}

// SetOpts allows Rpb...Req options to be set.
func (c *Client) SetOpts(opts interface{}) {
	c.opts = opts
}

// SetCoder sets the default Coder for structs.
func (c *Client) SetCoder(Coder *Coder) {
	c.Coder = Coder
}

// Close closes the node TCP connections.
func (c *Client) Close() {
	c.pool.Close()
}

// SelectNode selects a node from the pool, see *Pool.SelectNode()
func (c *Client) SelectNode() *Node {
	node := c.pool.SelectNode()
	return node
}

// Pool returns the pool associated with the client.
func (c *Client) Pool() *Pool {
	return c.pool
}

// ReqResp is the top level interface for the client for a bulk of Riak operations
func (c *Client) ReqResp(reqstruct interface{}, structname string, raw bool) (response interface{}, err error) {
	return c.SelectNode().ReqResp(reqstruct, structname, raw)
}

// ReqMultiResp is the top level interface for the client for the few
// operations which have to hit the server multiple times to guarantee
// a complete response: List keys, Map Reduce, etc.
func (c *Client) ReqMultiResp(reqstruct interface{}, structname string) (response interface{}, err error) {
	return c.SelectNode().ReqMultiResp(reqstruct, structname)
}

func (c *Client) EnableLogging() {
	c.logging = true
}

func (c *Client) DisableLogging() {
	c.logging = false
}

func (c *Client) LoggingEnabled() bool {
	return c.logging
}
