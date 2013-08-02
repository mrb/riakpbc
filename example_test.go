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

	// Dial all the nodes.
	if err := riak.Dial(); err != nil {
		log.Print(err.Error())
	}

	// Set Client ID
	if _, err := riak.SetClientId("coolio"); err != nil {
		log.Println(err.Error())
	}

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

	// Alternatively prepare and Do() queries.

	// Set Client ID
	opts1 := riak.NewSetClientIdRequest("coolio")
	if _, err := riak.Do(opts1); err != nil {
		log.Println(err.Error())
	}

	// Store raw data (int, string, []byte)
	opts2 := riak.NewStoreObjectRequest("bucket", "other")
	if _, err := riak.DoObject(opts2, "direct data"); err != nil {
		log.Println(err.Error())
	}

	// Fetch raw data (int, string, []byte)
	opts3 := riak.NewFetchObjectRequest("bucket", "other")
	objDo, err := riak.Do(opts3)
	if err != nil {
		log.Println(err.Error())
	}
	fmt.Println(string(objDo.(*RpbGetResp).GetContent()[0].GetValue()))
	// Output:
	// direct data

	// Close the connections if completely finished
	riak.Close()
}

func ExampleClientWithCoder() {
	// Initialize riakpbc against a 3 node cluster and with a JSON struct coder.
	//
	// Alternative marshallers can be built from this interface.
	coder := NewCoder("json", JsonMarshaller, JsonUnmarshaller)
	riakCoder := NewClientWithCoder([]string{"127.0.0.1:8087", "127.0.0.0:9089", "127.0.0.0:9090"}, coder)

	// Dial all the nodes.
	if err := riakCoder.Dial(); err != nil {
		log.Print(err.Error())
	}

	// Set Client ID
	if _, err := riakCoder.SetClientId("coolio"); err != nil {
		log.Println(err.Error())
	}

	// Store Struct (uses coder)
	data := ExampleData{
		Field1: "ExampleData1",
		Field2: 1,
	}
	if _, err := riakCoder.StoreStruct("bucket", "data", &data); err != nil {
		log.Println(err.Error())
	}

	// Fetch Struct (uses coder)
	out := &ExampleData{}
	if _, err := riakCoder.FetchStruct("bucket", "other", &out); err != nil {
		log.Println(err.Error())
	}
	fmt.Println(out.Field1)
	// Output
	// ExampleData1

	// Alternatively prepare and Do() queries.

	// Store Struct (uses coder)
	opts1 := riakCoder.NewStoreStructRequest("bucket", "data")
	if _, err := riakCoder.DoStruct(opts1, &data); err != nil {
		log.Println(err.Error())
	}

	// Fetch Struct (uses coder)
	outDo := &ExampleData{}
	opts2 := riakCoder.NewFetchStructRequest("bucket", "other")
	if _, err := riakCoder.DoStruct(opts2, &outDo); err != nil {
		log.Println(err.Error())
	}
	fmt.Println(outDo.Field1)
	// Output
	// ExampleData1

	// Close the connections if completely finished
	riakCoder.Close()
}
