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

```
func Dial(addr string) (*Conn, error)
    Dial connects to a single riak server.

func (c *Conn) FetchObject(bucket string, key string) (b []byte, err error)
    Fetch an object from a bucket

func (c *Conn) GetBucket(bucket string) (b []byte, err error)
    Get bucket info

func (c *Conn) GetServerInfo() (b []byte, err error)
    Get server info

func (c *Conn) ListBuckets() (b [][]byte, err error)
    List all buckets

func (c *Conn) ListKeys(bucket string) (b [][]byte, err error)
    List all keys from bucket

func (c *Conn) SetBucket(bucket string, nval *uint32, allowmult *bool) (b []byte, err error)
    Create bucket

func (c *Conn) StoreObject(bucket string, key string, content string) (b []byte, err error)
    Store an object in riak
```

### Benchmarkin'

`go test -test.bench=".*"`

### Credits

riakpbc is (c) Michael R. Bernstein, 2012

### Licensing

riakpbc is distributed under the MIT License, see `LICENSE` file for details.
