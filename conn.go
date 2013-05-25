package riakpbc

type Conn struct {
	cluster []string
	pool    *Pool
}

type Pool struct {
	nodes []*Node
}

func New(cluster []string) *Conn {
	return &Conn{
		cluster: cluster,
		pool:    newPool(cluster),
	}
}

func (c *Conn) Dial() error {
	for _, node := range c.pool.nodes {
		err := node.Dial()
		if err != nil {
			return err
		}
	}
	return nil
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
	node := pool.nodes[0]
	return node
}

func (pool *Pool) Write(request []byte) error {
	node := pool.SelectNode()
	return node.Write(request)
}

func (pool *Pool) Read() (response []byte, err error) {
	node := pool.SelectNode()
	return node.Read()
}

func (pool *Pool) Close() {
	for _, node := range pool.nodes {
		node.Close()
	}
}

func newPool(cluster []string) *Pool {
	var nodes []*Node

	for _, node := range cluster {
		inode := NewNode(node, 1e8, 1e8)
		nodes = append(nodes, inode)
	}

	pool := &Pool{
		nodes: nodes,
	}
	return pool
}
