riakpbc
=======

A Riak Protocol Buffer Client in Go.

### Notes

As of June 21, 2013 the API is deemed relatively stable.  The library should be considered to be at a 0.9 level release, with more minor changes pending.

### Basic Usage

    type Data struct {
    	Field1 string `riak:"index" json:"field1"`
    	Field2 int    `json:"field2"`
    }
    
    // Initialize riakpbc against a 3 node cluster
    riak := NewClient([]string{"127.0.0.1:8087", "127.0.0.0:9089", "127.0.0.0:9090"})

    // Add optional coder for storing JSON data to/from structs
    // Alternative marshallers can be built from this interface
    Coder := NewCoder("json", JsonMarshaller, JsonUnmarshaller)
    riak.SetCoder(Coder)

    // Dial all the nodes.
    if err := riak.Dial(); err != nil {
        log.Print(err.Error())
    }

    // Grab a node session.
    session := riak.Session()

    // Set Client ID
    if _, err := session.SetClientId("coolio"); err != nil {
        log.Println(err.Error())
    }

    // Store Struct (uses coder)
    data := ExampleData{
        Field1: "ExampleData1",
        Field2: 1,
    }
    if _, err := session.StoreStruct("bucket", "data", &data); err != nil {
        log.Println(err.Error())
    }

    // Fetch Struct (uses coder)
    out := &ExampleData{}
    if _, err := session.FetchStruct("bucket", "other", &out); err != nil {
        log.Println(err.Error())
    }

    // Store raw data (int, string, []byte)
    if _, err := session.StoreObject("bucket", "other", "direct data"); err != nil {
        log.Println(err.Error())
    }

    // Fetch raw data (int, string, []byte)
    obj, err := session.FetchObject("bucket", "other")
    if err != nil {
        log.Println(err.Error())
    }

    // Close the connections if completely finished
    riak.Close()

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
