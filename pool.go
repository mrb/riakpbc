package riakpbc

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	NODE_ERROR_THRESHOLD float64 = 0.1
	NODE_ERROR_MAX       float64 = 1.0
)

type Pool struct {
	nodes    map[string]*Node // index the node with its address string
	nodePool chan *Node
}

// NewPool returns an instantiated pool given a slice of node addresses.
func NewPool(cluster []string, coder *Coder) *Pool {
	rand.Seed(time.Now().UTC().UnixNano())
	nodeMap := make(map[string]*Node, len(cluster))

	for _, node := range cluster {
		newNode, err := NewNode(node, 10e8, 10e8)
		if err == nil {
			newNode.coder = coder
			nodeMap[node] = newNode
		}
	}

	pool := &Pool{
		nodes:    nodeMap,
		nodePool: make(chan *Node, len(nodeMap)),
	}

	for _, node := range nodeMap {
		pool.nodePool <- node
	}

	return pool
}

// SelectNode returns a node from the pool using weighted error selection.
//
// Each node has an assignable error rate, which is incremented when an error
// occurs, and decays over time - 50% each 10 seconds by default.
func (pool *Pool) SelectNode() *Node {
	for {
		// Pull a node off the pool and check it's health
		node := <-pool.nodePool
		if node.ok && node.ErrorRate() < NODE_ERROR_THRESHOLD {
			return node
		}
		// Node is not ok
		go func(p *Pool, n *Node) {
			// Loop until we are alive again
			for {
				// If the node is back below the threshold try to ping/reconnect again
				if n.ErrorRate() < NODE_ERROR_THRESHOLD {
					if n.DoPing() == false {
						// Still down, set back to max error
						n.RecordError(NODE_ERROR_MAX)
					} else {
						// Attempt to redial the node
						n.Close()
						if err := n.Dial(); err == nil {
							n.ok = true
							p.nodePool <- n // push it back to the pool
							return
						}
					}
				}
			}
		}(pool, node)
	}
}

func (pool *Pool) ReturnNode(node *Node) {
	pool.nodePool <- node
}

func (pool *Pool) DeleteNode(nodeKey string) {
	delete(pool.nodes, nodeKey)
}

func (pool *Pool) Close() {
	for _, node := range pool.nodes {
		node.Close()
	}
}

func (pool *Pool) Size() int {
	return len(pool.nodes)
}

func (pool *Pool) String() string {
	var outString string
	for _, node := range pool.nodes {
		nodeString := fmt.Sprintf(" [%s %f <%t>] ", node.addr, node.ErrorRate(), node.ok)
		outString += nodeString
	}
	return outString
}
