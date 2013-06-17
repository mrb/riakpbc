package riakpbc

import (
	"encoding/json"
	"fmt"
	"github.com/bmizerany/assert"
	"log"
	"testing"
)

type Data struct {
	Data string `json:"data"`
}

type RiakData struct {
	Email   string `json:"email" riak:"index"`
	Twitter string `json:"twitter" riak:"index"`
	Data    []byte `json:"data" riak:"index"`
}

func ExampleConn() {
	riak := New([]string{"127.0.0.1:8087", "127.0.0.0:918237198273"})

	Coder := NewCoder("json", JsonMarshaller, JsonUnmarshaller)
	riak.SetCoder(Coder)

	if err := riak.Dial(); err != nil {
		log.Print(err.Error())
	}

	// type Data struct {
	// 	Data string `json:"data"`
	// }
	_, err := riak.StoreStruct("bucket", "data", &Data{Data: "rules"})
	if err != nil {
		log.Println(err.Error())
	}

	_, err = riak.SetClientId("coolio")
	if err != nil {
		log.Println(err.Error())
	}

	/*
		id, err := riak.GetClientId()
		if err != nil {
			log.Println(err.Error())
		}
		fmt.Println(string(id.GetClientId()))\
	*/

	obj, err := riak.FetchObject("bucket", "data")
	if err != nil {
		log.Println(err.Error())
	}
	fmt.Println(string(obj.GetContent()[0].GetValue()))
	// Output:
	// {"data":"rules"}

	riak.Close()
}

func setupConnection(t *testing.T) (conn *Conn) {
	conn = New([]string{"127.0.0.1:8087",
		"127.0.0.1:8088",
		"127.0.0.1:8087",
		"127.0.0.1:8088"})
	var err error
	if err = conn.Dial(); err != nil {
		t.Error(err.Error())
	}
	assert.T(t, err == nil)

	Coder := NewCoder("json", JsonMarshaller, JsonUnmarshaller)
	conn.SetCoder(Coder)

	return conn
}

func setupSingleNodeConnection(t *testing.T) (conn *Conn) {
	conn = New([]string{"127.0.0.1:8087"})
	var err error
	if err = conn.Dial(); err != nil {
		t.Error(err.Error())
	}
	assert.T(t, err == nil)

	Coder := NewCoder("json", JsonMarshaller, JsonUnmarshaller)
	conn.SetCoder(Coder)

	return conn
}

func setupData(t *testing.T, conn *Conn) {
	ok, err := conn.StoreObject("riakpbctestbucket", "testkey", "{\"data\":\"is awesome!\"}")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, len(ok.GetKey()) == 0)
}

func teardownData(t *testing.T, conn *Conn) {
	ok, err := conn.DeleteObject("riakpbctestbucket", "testkey")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(ok) == "Success")
}

func TestStoreObject(t *testing.T) {
	riak := setupConnection(t)

	// Insert
	_, err := riak.StoreObject("riakpbctestbucket", "testkey_rpbcontent", &RpbContent{Value: []byte("rpbcontent data"), ContentType: []byte("text/plain")})
	if err != nil {
		t.Error(err.Error())
	}
	_, err = riak.StoreObject("riakpbctestbucket", "testkey_string", "string data")
	if err != nil {
		t.Error(err.Error())
	}
	_, err = riak.StoreObject("riakpbctestbucket", "testkey_int", 1000)
	if err != nil {
		t.Error(err.Error())
	}
	_, err = riak.StoreObject("riakpbctestbucket", "testkey_binary", []byte("binary data"))
	if err != nil {
		t.Error(err.Error())
	}

	// Cleanup
	_, err = riak.DeleteObject("riakpbctestbucket", "testkey_rpbcontent")
	if err != nil {
		t.Error(err.Error())
	}
	_, err = riak.DeleteObject("riakpbctestbucket", "testkey_string")
	if err != nil {
		t.Error(err.Error())
	}
	_, err = riak.DeleteObject("riakpbctestbucket", "testkey_int")
	if err != nil {
		t.Error(err.Error())
	}
	_, err = riak.DeleteObject("riakpbctestbucket", "testkey_binary")
	if err != nil {
		t.Error(err.Error())
	}
}

func TestStoreStruct(t *testing.T) {
	riak := setupConnection(t)

	riak_data := &RiakData{
		Email:   "riak@example.com",
		Twitter: "riak-twitter",
		Data:    []byte("riak-data"),
	}

	_, err := riak.StoreStruct("riakpbctestbucket", "testkey_struct", riak_data)
	if err != nil {
		t.Error(err.Error())
	}

	_, err = riak.DeleteObject("riakpbctestbucket", "testkey_struct")
	if err != nil {
		t.Error(err.Error())
	}
}

func TestStoreObjectWithOpts(t *testing.T) {
	riak := setupConnection(t)

	data, err := json.Marshal(&Data{Data: "is awesome!"})
	if err != nil {
		log.Println(err.Error())
	}

	z := new(bool)
	*z = true
	opts := &RpbPutReq{
		ReturnBody: z,
	}
	riak.SetOpts(opts)
	object, err := riak.StoreStruct("riakpbctestbucket", "testkeyopts", &Data{Data: "is awesome!"})
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(object.GetContent()[0].GetValue()) == string(data))

	_, err = riak.DeleteObject("riakpbctestbucket", "testkeyopts")
	if err != nil {
		t.Error(err.Error())
	}
}

func TestFetchObject(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	object, err := riak.FetchObject("riakpbctestbucket", "testkey")
	if err != nil {
		t.Error(err.Error())
	}
	stringObject := string(object.GetContent()[0].GetValue())

	data := "{\"data\":\"is awesome!\"}"
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, stringObject == data)

	teardownData(t, riak)
}

func TestFetchStruct(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	riak_data := &RiakData{
		Email:   "riak@example.com",
		Twitter: "riak-twitter",
		Data:    []byte("riak-data"),
	}

	_, err := riak.StoreStruct("riakpbctestbucket", "testkey_struct", riak_data)
	if err != nil {
		t.Error(err.Error())
	}

	// Test
	data := &RiakData{}
	err = riak.FetchStruct("riakpbctestbucket", "testkey_struct", data)
	if err != nil {
		t.Error(err.Error())
	}

	_, err = riak.DeleteObject("riakpbctestbucket", "testkey_struct")
	if err != nil {
		t.Error(err.Error())
	}

	teardownData(t, riak)
}

func TestDeleteObject(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	object, err := riak.DeleteObject("riakpbctestbucket", "testkey")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(object) == "Success")

	_, err = riak.FetchObject("riakpbctestbucket", "testkey")
	assert.T(t, err.Error() == "object not found")

	teardownData(t, riak)
}
