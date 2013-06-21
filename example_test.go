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

	// Set Client ID
	if _, err := riak.SetClientId("coolio"); err != nil {
		log.Println(err.Error())
	}

	// Store Struct (uses coder)
	data := ExampleData{
		Field1: "ExampleData1",
		Field2: 1,
	}
	if _, err := riak.StoreStruct("bucket", "data", &data); err != nil {
		log.Println(err.Error())
	}

	// Fetch Struct (uses coder)
	out := &ExampleData{}
	if err := riak.FetchStruct("bucket", "other", &out); err != nil {
		log.Println(err.Error())
	}
	fmt.Println(out.Field1)
	// Output
	// ExampleData1

	// Store raw data (int, string, []byte)
	if _, err := riak.StoreObject("bucket", "other", "direct data"); err != nil {
		log.Println(err.Error())
	}

	// Fetch raw data (int, string, []byte)
	obj, err := riak.FetchObject("bucket", "other")
	if err != nil {
		log.Println(err.Error())
	}
	fmt.Println(string(obj.GetContent()[0].GetValue()))
	// Output:
	// direct data

	// Close the connections if completely finished
	riak.Close()
}
