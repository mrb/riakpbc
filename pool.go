package riakpbc

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

func (pool *Pool) SelectNode() *Node {
	pool.Lock()
	errorThreshold := 0.1
	var possibleNodes []*Node

	for _, node := range pool.nodes {
		nodeErrorValue := node.ErrorRate()

		if nodeErrorValue < errorThreshold {
			possibleNodes = append(possibleNodes, node)
		}
	}

	numPossibleNodes := len(possibleNodes)

	var chosenNode *Node
	if numPossibleNodes > 0 {
		chosenNode = possibleNodes[rand.Int31n(int32(numPossibleNodes))]
	} else {
		chosenNode = pool.RandomNode()
	}

	resp, err := chosenNode.ReqResp([]byte{}, "RpbPingReq", true)
	if resp == nil || string(resp.([]byte)) != "Pong" || err != nil {
		chosenNode.RecordError(1.0)
		chosenNode.Dial()
		//pool.DeleteNode(chosenNode.addr)
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

func (pool *Pool) Stats() {
	log.Print(pool.nodes)
}

func (pool *Pool) String() string {
	var outString string
	for _, node := range pool.nodes {
		nodeString := fmt.Sprintf(" [%s %f] ", node.addr, node.ErrorRate())
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

	return pool
}
