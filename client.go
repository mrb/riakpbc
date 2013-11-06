package riakpbc

import (
	"errors"
	"log"
	"time"
)

type Client struct {
	cluster       []string
	pool          *Pool
	Coder         *Coder // Coder for (un)marshalling data
	logging       bool
	pingFrequency int
	isClosed      bool
	closeChannel  chan bool
}

// NewClient accepts a slice of node address strings and returns a Client object.
//
// Illegally addressed nodes will be rejected in the NewPool call.
func NewClient(cluster []string) *Client {
	return &Client{
		cluster:       cluster,
		pool:          NewPool(cluster),
		logging:       false,
		pingFrequency: 1000,
		closeChannel:  make(chan bool),
		isClosed:      false,
	}
}

// NewClientWihtCoder accepts a slice of node address strings, a Coder for processing structs into data, and returns a Client object.
//
// Illegally addressed nodes will be rejected in the NewPool call.
func NewClientWithCoder(cluster []string, coder *Coder) *Client {
	return &Client{
		cluster:       cluster,
		pool:          NewPool(cluster),
		Coder:         coder,
		logging:       false,
		pingFrequency: 1000,
		closeChannel:  make(chan bool),
		isClosed:      false,
	}
}

// Dial connects all nodes in the pool to their addresses via TCP.
//
// Nodes which are down get set to redial in the background.
func (c *Client) Dial() error {
	c.closeChannel = make(chan bool)
	c.isClosed = false

	for _, node := range c.pool.nodes {
		err := node.Dial()
		if err != nil {
			node.RecordError(10.0)
			if c.LoggingEnabled() {
				log.Print("[POOL] Error: ", err)
			}
		}
	}

	if c.pool.Size() < 1 {
		return ErrZeroNodes
	}

	go c.BackgroundNodePing()

	return nil
}

// Close closes the node TCP connections.
func (c *Client) Close() error {
	if c.isClosed {
		return errors.New("Client has been closed.")
	}

	c.closeChannel <- true
	c.isClosed = true
	c.pool.Close()
	close(c.closeChannel)
	return nil
}

func (c *Client) BackgroundNodePing() {
	for {
		select {
		case <-time.After(time.Duration(c.pingFrequency) * time.Millisecond):
			c.pool.Ping()
		case <-c.closeChannel:
			return
		}
	}
}

// SelectNode selects a node from the pool, see *Pool.SelectNode()
func (c *Client) SelectNode() (*Node, error) {
	return c.pool.SelectNode()
}

// Pool returns the pool associated with the client.
func (c *Client) Pool() *Pool {
	return c.pool
}

// Do executes a prepared query and returns the results.
func (c *Client) Do(opts interface{}) (interface{}, error) {
	// Bucket
	if _, ok := opts.(*RpbListKeysReq); ok {
		return c.listKeys(opts.(*RpbListKeysReq), string(opts.(*RpbListKeysReq).GetBucket()))
	}
	if _, ok := opts.(*RpbGetBucketReq); ok {
		return c.getBucket(opts.(*RpbGetBucketReq), string(opts.(*RpbGetBucketReq).GetBucket()))
	}
	if _, ok := opts.(*RpbSetBucketReq); ok {
		nval := opts.(*RpbSetBucketReq).Props.GetNVal()
		allowMulti := opts.(*RpbSetBucketReq).Props.GetAllowMult()
		return c.setBucket(opts.(*RpbSetBucketReq), string(opts.(*RpbSetBucketReq).GetBucket()), &nval, &allowMulti)
	}

	// Object
	if _, ok := opts.(*RpbGetReq); ok {
		return c.fetchObject(opts.(*RpbGetReq), string(opts.(*RpbGetReq).GetBucket()), string(opts.(*RpbGetReq).GetKey()))
	}
	if _, ok := opts.(*RpbDelReq); ok {
		return c.deleteObject(opts.(*RpbDelReq), string(opts.(*RpbDelReq).GetBucket()), string(opts.(*RpbDelReq).GetKey()))
	}

	// Query
	if _, ok := opts.(*RpbMapRedReq); ok {
		return c.mapReduce(opts.(*RpbMapRedReq), string(opts.(*RpbMapRedReq).GetRequest()), string(opts.(*RpbMapRedReq).GetContentType()))
	}
	if _, ok := opts.(*RpbIndexReq); ok {
		return c.index(opts.(*RpbIndexReq), string(opts.(*RpbIndexReq).GetBucket()), string(opts.(*RpbIndexReq).GetIndex()), string(opts.(*RpbIndexReq).GetKey()), string(opts.(*RpbIndexReq).GetRangeMin()), string(opts.(*RpbIndexReq).GetRangeMax()))
	}
	if _, ok := opts.(*RpbSearchQueryReq); ok {
		return c.search(opts.(*RpbSearchQueryReq), string(opts.(*RpbSearchQueryReq).GetIndex()), string(opts.(*RpbSearchQueryReq).GetQ()))
	}

	// Server
	if _, ok := opts.(*RpbSetClientIdReq); ok {
		return c.setClientId(opts.(*RpbSetClientIdReq), string(opts.(*RpbSetClientIdReq).GetClientId()))
	}

	return nil, nil
}

// DoObject executes a prepared query with data and returns the results.
func (c *Client) DoObject(opts interface{}, in interface{}) (interface{}, error) {
	if _, ok := opts.(*RpbPutReq); ok {
		return c.storeObject(opts.(*RpbPutReq), string(opts.(*RpbPutReq).GetBucket()), string(opts.(*RpbPutReq).GetKey()), in)
	}

	return nil, nil
}

// DoStruct executes a prepared query on a struct with the coder and returns the results.
func (c *Client) DoStruct(opts interface{}, in interface{}) (interface{}, error) {
	if _, ok := opts.(*RpbGetReq); ok {
		return c.fetchStruct(opts.(*RpbGetReq), string(opts.(*RpbGetReq).GetBucket()), string(opts.(*RpbGetReq).GetKey()), in)
	}
	if _, ok := opts.(*RpbPutReq); ok {
		return c.storeStruct(opts.(*RpbPutReq), string(opts.(*RpbPutReq).GetBucket()), string(opts.(*RpbPutReq).GetKey()), in)
	}

	return nil, nil
}

// ReqResp is the top level interface for the client for a bulk of Riak operations
func (c *Client) ReqResp(reqstruct interface{}, structname string, raw bool) (response interface{}, err error) {
	node, err := c.SelectNode()
	if err != nil {
		return nil, err
	}
	return node.ReqResp(reqstruct, structname, raw)
}

// ReqMultiResp is the top level interface for the client for the few
// operations which have to hit the server multiple times to guarantee
// a complete response: List keys, Map Reduce, etc.
func (c *Client) ReqMultiResp(reqstruct interface{}, structname string) (response interface{}, err error) {
	node, err := c.SelectNode()
	if err != nil {
		return nil, err
	}
	return node.ReqMultiResp(reqstruct, structname)
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
