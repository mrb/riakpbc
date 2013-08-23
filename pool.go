package riakpbc

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	NODE_WRITE_RETRY     time.Duration = time.Second * 10 // 10s
	NODE_READ_RETRY      time.Duration = time.Second * 10 // 10s
	NODE_ERROR_THRESHOLD float64       = 0.5
)

type Pool struct {
	nodes map[string]*Node // index the node with its address string
	sync.Mutex
}

// NewPool returns an instantiated pool given a slice of node addresses.
func NewPool(cluster []string) *Pool {
	rand.Seed(time.Now().UTC().UnixNano())
	nodeMap := make(map[string]*Node, len(cluster))

	for _, node := range cluster {
		newNode, err := NewNode(node, NODE_READ_RETRY, NODE_WRITE_RETRY)
		if err == nil {
			nodeMap[node] = newNode
		}
	}

	pool := &Pool{
		nodes: nodeMap,
	}

	return pool
}

// SelectNode returns a node from the pool using weighted error selection.
//
// Each node has an assignable error rate, which is incremented when an error
// occurs, and decays over time - 50% each 10 seconds by default.
func (pool *Pool) SelectNode() (*Node, error) {
	pool.Lock()
	defer pool.Unlock()

	var possibleNodes []*Node
	for _, node := range pool.nodes {
		if node.ErrorRate() < NODE_ERROR_THRESHOLD {
			possibleNodes = append(possibleNodes, node)
		}
	}

	count := len(possibleNodes)

	if count > 0 {
		return possibleNodes[rand.Int31n(int32(count))], nil
	}

	return nil, ErrAllNodesDown
}

func (pool *Pool) Ping() {
	pool.Lock()
	defer pool.Unlock()

	for _, node := range pool.nodes {
		nodeGood := node.Ping()
		if nodeGood == false {
			node.RecordError(0.1)
			node.Lock()
			node.Close()
			node.Dial()
			node.Unlock()
		} else {
			node.SetOk(true)
		}

	}
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
		nodeString := fmt.Sprintf(" [%s %f <%t>] ", node.addr, node.ErrorRate(), node.GetOk())
		outString += nodeString
	}
	return outString
}
