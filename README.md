riakpbc
=======

A Riak Protocol Buffer Client in Go.

A simple `riakpbc` program:

```go
package main

import (
	"github.com/kr/pretty"
	"log"
	"mrb/riakpbc"
)

func main() {
	// connect to the riak cluster
	riak, err := riakpbc.Dial("127.0.0.1:8081")

	if err != nil {
		log.Print(err)
		return
	}

	// get the value of 'key' in the 'bucket' bucket and print it
	obj, _ := riak.FetchObject("bucket", "key")
	log.Printf("%s", pretty.Formatter(obj))
}
```

The rest of the API:

```go
func Dial(addr string) (*Conn, error)
    //Dial connects to a single riak server.

func (c *Conn) Close()
    //Close the connection

func (c *Conn) DeleteObject(bucket string, key string) (response []byte, err error)
    //Delete an Object from a bucket

func (c *Conn) FetchObject(bucket string, key string) (response []byte, err error)
    //Fetch an object from a bucket

func (c *Conn) GetBucket(bucket string) (response []byte, err error)
    //Get bucket info

func (c *Conn) GetClientId() (response []byte, err error)
    //Get client ID

func (c *Conn) GetServerInfo() (response []byte, err error)
    //Get server info

func (c *Conn) ListBuckets() (response [][]byte, err error)
    //List all buckets

func (c *Conn) ListKeys(bucket string) (response [][]byte, err error)
    //List all keys from bucket

func (c *Conn) MapReduce(content string) (response [][]byte, err error)
    //Send a JSON MapReduce query to the server

func (c *Conn) Ping() (response []byte, err error)
    //Ping the server

func (c *Conn) SetBucket(bucket string, nval *uint32, allowmult *bool) (response []byte, err error)
    //Create bucket

func (c *Conn) SetClientId(clientId string) (response []byte, err error)
    //Set client ID

func (c *Conn) StoreObject(bucket string, key string, content string) (response []byte, err error)
    //Store an object in riak
```

_MapReduce and ListKeys are under development_

The following errors are defined:

```go
	ErrLengthZero     = errors.New("length response 0")
	ErrCorruptHeader  = errors.New("corrupt header")
	ErrObjectNotFound = errors.New("object not found")
	ErrNoSuchCommand  = errors.New("no such command")
	ErrBucketExists   = errors.New("bucket exists")
	ErrRiakError      = errors.New("riak error")
	ErrNotDone        = errors.New("not done")
	ErrReadTimeout    = errors.New("read timeout")
	ErrWriteTimeout   = errors.New("write timeout")
```

### Benchmarkin'

`go test -test.bench=".*"`

### Credits

riakpbc is (c) Michael R. Bernstein, 2012

### Licensing

riakpbc is distributed under the MIT License, see `LICENSE` file for details.
