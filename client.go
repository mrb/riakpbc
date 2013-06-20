package riakpbc

import (
	"log"
	"sync"
)

type Client struct {
	cluster []string
	pool    *Pool
	opts    interface{} // potential Rpb...Req opts
	Coder   *Coder      // Coder for (un)marshalling data
	optsMu  *sync.Mutex
}

type Pool struct {
	nodes   map[string]*Node // index the node with its address string
	current *Node
	sync.Mutex
}

// NewClient accepts a slice of node address strings and returns a Client object.
func NewClient(cluster []string) *Client {
	return &Client{
		cluster: cluster,
		pool:    newPool(cluster),
		optsMu:  &sync.Mutex{},
	}
}

// Dial connects all nodes in the pool to their addresses via TCP.
//
// Illegally addressed nodes will be rejected here.
func (c *Client) Dial() error {
	for k, node := range c.pool.nodes {
		err := node.Dial()
		if err != nil {
			log.Print("[POOL] Error: ", err)
			c.pool.DeleteNode(k)
		}
	}

	log.Print("[POOL] Riak Dialed. Connected to ", c.pool.Size(), " Riak nodes.")

	if c.pool.Size() < 1 {
		return ErrZeroNodes
	}

	return nil
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

func (c *Client) ReqResp(reqstruct interface{}, structname string, raw bool) (response interface{}, err error) {
	return c.SelectNode().ReqResp(reqstruct, structname, raw)
}
