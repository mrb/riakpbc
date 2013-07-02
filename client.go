package riakpbc

import (
	"log"
)

type Client struct {
	cluster []string
	pool    *Pool
	Coder   *Coder // Coder for (un)marshalling data
	logging bool
}

// NewClient accepts a slice of node address strings and returns a Client object.
func NewClient(cluster []string) *Client {
	return &Client{
		cluster: cluster,
		pool:    NewPool(cluster),
		logging: false,
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

	return nil
}

// Session requests a new Node to temporarily work with.
func (c *Client) Session() *Node {
	return c.pool.SelectNode()
}

// SetCoder sets the default Coder for structs.
func (c *Client) SetCoder(coder *Coder) {
	for i, _ := range c.pool.nodes {
		c.pool.nodes[i].Coder = c.Coder
	}
}

// Close closes the node TCP connections.
func (c *Client) Close() {
	c.pool.Close()
}

// Pool returns the pool associated with the client.
func (c *Client) Pool() *Pool {
	return c.pool
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
