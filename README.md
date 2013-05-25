riakpbc
=======

## This Library is under heavy development as of 05/25/2013. Please execrcise caution.

A Riak Protocol Buffer Client in Go.

A simple `riakpbc` program:

```go
package main

import (
	"log"
	"mrb/riakpbc"
)

func main() {
	riak, err := riakpbc.New("127.0.0.1:8087", 1e8, 1e8)

	if err != nil {
		log.Print(err)
		return
	}

	err = riak.Dial()
	if err != nil {
		log.Print(err)
		return
	}

	ok, err := riak.StoreObject("buckey", "bro", "{'data':'rules'}")
	log.Print(string(ok), " - ", err)

	ok, err = riak.SetClientId("coolio")
	log.Print(string(ok), " - ", err)

	ok, err = riak.GetClientId()
	log.Print(string(ok), " - ", err)

	obj, err := riak.FetchObject("buckey", "bro")
	log.Print(string(obj), " - ", err)

	riak.Close()
}
```

See `example/riakpbc.go` for more usage.

The rest of the API:

```go
func New(addr string, readTimeout time.Duration, writeTimeout time.Duration) (*Conn, error)
    //Returns a new Conn connection

func (c *Conn) Dial() (err error)
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
### Documentin'

`http://go.pkgdoc.org/github.com/mrb/riakpbc` or `go doc`

### Testin'

`go test`

### Benchmarkin'

`go test -test.bench=".*"`

### Disclaimin'

I am brand new to Go, and also to Riak, and also to Protocol Buffers for that matter. If I am doing something incredibly stupid or misguided here, please help me out with a pull request, an issue, or by hitting me up on IRC (mrb_bk on #freenode). Thanks!

### Exemplifyin'

There's an example app here: https://github.com/mrb/shoebox and an example in the `example` directory.

### Creditin'

riakpbc is (c) Michael R. Bernstein, 2012

### Licensin'

riakpbc is distributed under the MIT License, see `LICENSE` file for details.
