riakpbc
=======

A Riak Protocol Buffer Client in Go.

### Notes

As of June 21, 2013 the API is deemed relatively stable.  The library should be considered to be at a 0.9 level release, with more minor changes pending.

### Installation

	$ go get github.com/mrb/riakpbc

### Basic Usage

    package main
	import (
		"fmt"
		"log"
		riak "github.com/mrb/riakpbc"
	)

	type Data struct {
		Field1 string `riak:"index" json:"field1"`
		Field2 int    `json:"field2"`
	}

	func main() {
		// Initialize riakpbc against a 3 node cluster
		r := riak.NewClient([]string{"127.0.0.1:8087", "127.0.0.0:9089", "127.0.0.0:9090"})

		// Add optional coder for storing JSON data to/from structs
		// Alternative marshallers can be built from this interface
		Coder := riak.NewCoder("json", riak.JsonMarshaller, riak.JsonUnmarshaller)
		r.SetCoder(Coder)

		// Dial all the nodes.
		if err := r.Dial(); err != nil {
			log.Fatalln(err.Error())
		}

		// Set Client ID
		if _, err := r.SetClientId("coolio"); err != nil {
			log.Fatalln(err.Error())
		}

		// Store Struct (uses coder)
		data := Data{
			Field1: "ExampleData1",
			Field2: 1,
		}
		if _, err := r.StoreStruct("bucket", "data", &data); err != nil {
			log.Fatalln(err.Error())
		}

		// Fetch Struct (uses coder)
		out := Data{}
		if _, err := r.FetchStruct("bucket", "data", &out); err != nil {
			log.Fatalln(err.Error())
		}
		fmt.Println(out.Field1) // ExampleData1

		// Store raw data (int, string, []byte)
		if _, err := r.StoreObject("bucket", "other", "direct data"); err != nil {
			log.Fatalln(err.Error())
		}

		// Fetch raw data (int, string, []byte)
		obj, err := r.FetchObject("bucket", "other")
		if err != nil {
			log.Fatalln(err.Error())
		}
		fmt.Println(string(obj.GetContent()[0].GetValue())) // direct data

		// Close the connections if completely finished
		r.Close()
	}

### Documentation

http://godoc.org/github.com/mrb/riakpbc or `go doc`

### Testing

`go test`

### Benchmarks

`go test -test.bench=".*"`

### Credits

* Michael R. Bernstein (@mrb)
* Brian Jones (@boj)

### License

riakpbc is distributed under the MIT License, see `LICENSE` file for details.
