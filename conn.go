package riakpbc

import (
	"log"
	"math/rand"
	"time"
)

type Conn struct {
	cluster []string
	pool    *Pool
	opts    interface{} // potential Rpb...Req opts
}

type Pool struct {
	nodes map[string]*Node // index the node with its address string
}

func New(cluster []string) *Conn {
	return &Conn{
		cluster: cluster,
		pool:    newPool(cluster),
	}
}

func (c *Conn) Dial() error {
	for k, node := range c.pool.nodes {
		err := node.Dial()
		if err != nil {
			c.pool.DeleteNode(k)
		}
	}

	log.Print("[POOL] Riak Dialed. Connected to ", len(c.pool.nodes), " Riak nodes.")

	return nil
}

// Opts returns the set options, and reests them internally to nil.
func (c *Conn) Opts() interface{} {
	opts := c.opts
	c.opts = nil
	return opts
}

// SetOpts allows Rpb...Req options to be set.
func (c *Conn) SetOpts(opts interface{}) {
	c.opts = opts
}

func (c *Conn) Write(request []byte) error {
	return c.pool.Write(request)
}

func (c *Conn) Read() (response []byte, err error) {
	return c.pool.Read()
}

func (c *Conn) Close() {
	c.pool.Close()
}

func (pool *Pool) SelectNode() *Node {
	var selectedNode *Node

	var randVal float32
	randVal = 0

	for _, node := range pool.nodes {
		throwAwayRand := rand.Float32()

		if throwAwayRand > randVal {
			selectedNode = node
			randVal = throwAwayRand
		}
	}
	return selectedNode
}

func (pool *Pool) DeleteNode(nodeKey string) {
	delete(pool.nodes, nodeKey)

	var nodeStrings []string

	for k, _ := range pool.nodes {
		nodeStrings = append(nodeStrings, k)
	}

	log.Print("[POOL] Node ", nodeKey, " deleted. New pool consists of ", nodeStrings)
	return
}

func (pool *Pool) Write(request []byte) error {
	node := pool.SelectNode()
	log.Print("[WRITE] To Node ", node.addr)
	return node.Write(request)
}

func (pool *Pool) Read() (response []byte, err error) {
	node := pool.SelectNode()
	log.Print("[READ] From Node ", node.addr)
	return node.Read()
}

func (pool *Pool) Close() {
	for _, node := range pool.nodes {
		node.Close()
	}
}

func newPool(cluster []string) *Pool {
	rand.Seed(time.Now().UTC().UnixNano())
	nodeMap := make(map[string]*Node, len(cluster))

	for _, node := range cluster {
		nodeMap[node] = NewNode(node, 1e8, 1e8)
	}

	pool := &Pool{
		nodes: nodeMap,
	}

	log.Print("[POOL] New connection Pool established. Attempting connection to ", len(pool.nodes), " Riak nodes.")

	return pool
}
