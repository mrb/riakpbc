package riakpbc

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

type Conn struct {
	cluster []string
	pool    *Pool
	current *Node
	opts    interface{} // potential Rpb...Req opts
	Coder   *Coder      // Coder for (un)marshalling data
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

// Opts returns the set options, and reests them internally to nil.
func (c *Conn) Opts() interface{} {
	opts := c.opts
	c.opts = nil
	return opts
}

func (c *Conn) Current() *Node {
	return c.current
}

func (c *Conn) RecordError(amount float64) {
	c.Current().errorRate.Add(amount)
}

// SetOpts allows Rpb...Req options to be set.
func (c *Conn) SetOpts(opts interface{}) {
	c.opts = opts
}

// SetCoder sets the default Coder for structs.
func (c *Conn) SetCoder(Coder *Coder) {
	c.Coder = Coder
}

func (c *Conn) Write(request []byte) error {
	return c.current.Write(request)
}

func (c *Conn) Read() (response []byte, err error) {
	return c.current.Read()
}

func (c *Conn) Close() {
	c.pool.Close()
}

func (c *Conn) SelectNode() {
	c.current = c.pool.SelectNode()
	c.current.Dial()
}

func (c *Conn) Pool() *Pool {
	return c.pool
}

func (pool *Pool) SelectNode() *Node {
	errorThreshold := 0.1
	var possibleNodes []*Node

	for _, node := range pool.nodes {
		nodeErrorValue := node.ErrorRate()

		if nodeErrorValue < errorThreshold {
			possibleNodes = append(possibleNodes, node)
		}
	}

	numPossibleNodes := len(possibleNodes)

	if numPossibleNodes > 0 {
		return possibleNodes[rand.Int31n(int32(numPossibleNodes))]
	} else {
		return pool.RandomNode()
	}
}

func (pool *Pool) RandomNode() *Node {
	var randomNode *Node

	var randVal float32
	randVal = 0

	for _, node := range pool.nodes {
		throwAwayRand := rand.Float32()

		if throwAwayRand > randVal {
			randomNode = node
			randVal = throwAwayRand
		}
	}

	return randomNode
}

func (pool *Pool) DeleteNode(nodeKey string) {
	delete(pool.nodes, nodeKey)

	var nodeStrings []string

	for k, _ := range pool.nodes {
		nodeStrings = append(nodeStrings, k)
	}

	return
}

func (pool *Pool) Close() {
	for _, node := range pool.nodes {
		node.Close()
	}
}

func (pool *Pool) Size() int {
	return len(pool.nodes)
}

func (pool *Pool) Stats() {
	log.Print(pool.nodes)
}

func (pool *Pool) String() string {
	var outString string
	for _, node := range pool.nodes {
		nodeString := fmt.Sprintf(" [%s %f] ", node.addr, node.errorRate.Value())
		outString += nodeString
	}
	return outString
}

func newPool(cluster []string) *Pool {
	rand.Seed(time.Now().UTC().UnixNano())
	nodeMap := make(map[string]*Node, len(cluster))

	for _, node := range cluster {
		newNode, err := NewNode(node, 10e8, 10e8)
		if err == nil {
			nodeMap[node] = newNode
		} else {
			log.Print("[POOL] Node rejected from pool. Error: ", err, " Node: ", node)
		}
	}

	pool := &Pool{
		nodes: nodeMap,
	}

	log.Print("[POOL] New connection Pool established. Attempting connection to ", len(pool.nodes), " Riak nodes.")

	pool.SelectNode()

	return pool
}
