package riakpbc

import (
	"fmt"
	"log"
)

type ExampleData struct {
	Field1 string `riak:"index" json:"field1"`
	Field2 int    `json:"field2"`
}

func ExampleClient() {
	// Add optional coder for storing JSON data to/from structs
	// Alternative marshallers can be built from this interface
	coder := NewCoder("json", JsonMarshaller, JsonUnmarshaller)

	// Initialize riakpbc against a 3 node cluster
	riak := NewClient([]string{"127.0.0.1:8087", "127.0.0.0:9089", "127.0.0.0:9090"}, coder)

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
	fmt.Println(out.Field1)
	// Output
	// ExampleData1

	// Store raw data (int, string, []byte)
	if _, err := session.StoreObject("bucket", "other", "direct data"); err != nil {
		log.Println(err.Error())
	}

	// Fetch raw data (int, string, []byte)
	obj, err := session.FetchObject("bucket", "other")
	if err != nil {
		log.Println(err.Error())
	}
	fmt.Println(string(obj.GetContent()[0].GetValue()))
	// Output:
	// direct data

	// All sessions must be Free'ed
	riak.Free(session)

	// Close the connections if completely finished
	riak.Close()
}
