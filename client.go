package riakpbc

import (
	"log"
)

type Client struct {
	cluster []string
	pool    *Pool
	logging bool
}

// NewClient accepts a slice of node address strings and returns a Client object.
func NewClient(cluster []string, coder *Coder) *Client {
	return &Client{
		cluster: cluster,
		pool:    NewPool(cluster, coder),
		logging: false,
	}
}

// Dial connects all nodes in the pool to their addresses via TCP.
//
// Illegally addressed nodes will be rejected here.
func (self *Client) Dial() error {
	for k, node := range self.pool.nodes {
		err := node.Dial()
		if err != nil {
			if self.LoggingEnabled() {
				log.Print("[POOL] Error: ", err)
			}
			self.pool.DeleteNode(k)
		}
	}

	if self.LoggingEnabled() {
		log.Print("[POOL] Riak Dialed. Connected to ", self.pool.Size(), " Riak nodes.")
	}

	if self.pool.Size() < 1 {
		return ErrZeroNodes
	}

	return nil
}

// Session requests a new Node to temporarily work with from the pool.
func (self *Client) Session() *Node {
	return self.pool.SelectNode()
}

// Free must be called on a Node to return it back to the pool.
func (self *Client) Free(node *Node) {
	self.pool.ReturnNode(node)
}

// Close closes the node TCP connections.
func (self *Client) Close() {
	self.pool.Close()
}

// Pool returns the pool associated with the client.
func (self *Client) Pool() *Pool {
	return self.pool
}

func (self *Client) EnableLogging() {
	self.logging = true
}

func (self *Client) DisableLogging() {
	self.logging = false
}

func (self *Client) LoggingEnabled() bool {
	return self.logging
}
