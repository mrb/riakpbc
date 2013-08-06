package riakpbc

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	NODE_WRITE_RETRY         time.Duration = time.Second * 5 // 5s
	NODE_READ_RETRY          time.Duration = time.Second * 5 // 5s
	NODE_DOWN_RETRY          time.Duration = time.Second / 2 // 0.5s
	NODE_DOWN_MAX_RETRY      time.Duration = time.Second * 5 // 5s
	NODE_DOWN_RETRY_INCREMET time.Duration = time.Second / 2 // 0.5s
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
		newNode, err := NewNode(node, NODE_READ_RETRY, NODE_WRITE_RETRY, NODE_DOWN_RETRY)
		if err == nil {
			nodeMap[node] = newNode
		}
	}

	pool := &Pool{
		nodes: nodeMap,
	}

	return pool
}

// SelectNode returns a node from the pool.
func (pool *Pool) SelectNode() (*Node, error) {
	pool.Lock()
	defer pool.Unlock()

	var possibleNodes []*Node
	for _, node := range pool.nodes {
		if node.GetOk() {
			possibleNodes = append(possibleNodes, node)
		}
	}

	if len(possibleNodes) > 0 {
		return possibleNodes[rand.Int31n(int32(len(possibleNodes)))], nil
	}

	return nil, ErrAllNodesDown
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
		nodeString := fmt.Sprintf(" [%s %f <%t>] ", node.addr, node.GetOk())
		outString += nodeString
	}
	return outString
}
