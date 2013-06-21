package riakpbc

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Pool struct {
	nodes   map[string]*Node // index the node with its address string
	current *Node
	sync.Mutex
}

// NewPool returns an instantiated pool given a slice of node addresses.
func NewPool(cluster []string) *Pool {
	rand.Seed(time.Now().UTC().UnixNano())
	nodeMap := make(map[string]*Node, len(cluster))

	for _, node := range cluster {
		newNode, err := NewNode(node, 10e8, 10e8)
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
func (pool *Pool) SelectNode() *Node {
	pool.Lock()
	errorThreshold := 0.1
	var possibleNodes []*Node

	for _, node := range pool.nodes {
		nodeErrorValue := node.ErrorRate()

		if nodeErrorValue < errorThreshold {
			possibleNodes = append(possibleNodes, node)
		} else {
			if node.ok == false && node.ErrorRate() < 100.0 {
				go func(iNode *Node) {
					nodeGood := iNode.Ping()
					if nodeGood == false {
            iNode.RecordError(100.0)
						iNode.Lock()
						iNode.Close()
						iNode.Dial()
						iNode.Unlock()
					} else {
						iNode.ok = true
					}
				}(node)
			}
		}
	}

	numPossibleNodes := len(possibleNodes)

	var chosenNode *Node
	if numPossibleNodes > 0 {
		chosenNode = possibleNodes[rand.Int31n(int32(numPossibleNodes))]
	} else {
		chosenNode = pool.RandomNode()
	}

	pool.current = chosenNode
	pool.Unlock()

	return chosenNode
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
}

func (pool *Pool) Close() {
	for _, node := range pool.nodes {
		node.Close()
	}
}

func (pool *Pool) Current() *Node {
	node := pool.current
	return node
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
