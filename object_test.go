package riakpbc

import (
	//"encoding/json"
	"fmt"
	"github.com/bmizerany/assert"
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

func setupConnection(t *testing.T) (client *Client) {
	client = NewClient([]string{"127.0.0.1:8087",
		"127.0.0.1:8088",
		"127.0.0.1:8087",
		"127.0.0.1:8088"})
	var err error
	if err = client.Dial(); err != nil {
		t.Error(err.Error())
	}
	assert.T(t, err == nil)

	Coder := NewCoder("json", JsonMarshaller, JsonUnmarshaller)
	client.SetCoder(Coder)

	return client
}

func setupSingleNodeConnection(t *testing.T) (client *Client) {
	client = NewClient([]string{"127.0.0.1:8087"})
	var err error
	if err = client.Dial(); err != nil {
		t.Error(err.Error())
	}
	assert.T(t, err == nil)

	Coder := NewCoder("json", JsonMarshaller, JsonUnmarshaller)
	client.SetCoder(Coder)

	return client
}

func setupData(t *testing.T, client *Client) {
	ok, err := client.StoreObject("riakpbctestbucket", "testkey", "{\"data\":\"is awesome!\"}")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, len(ok.GetKey()) == 0)
}

func teardownData(t *testing.T, client *Client) {
	ok, err := client.DeleteObject("riakpbctestbucket", "testkey")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(ok) == "Success")
}

func TestHead(t *testing.T) {
	riak := setupConnection(t)
	userMeta := []*RpbPair{&RpbPair{Key: []byte("meta"), Value: []byte("schmeta")}}
	rpbObj := &RpbContent{Value: []byte("rpbcontent data"), ContentType: []byte("text/plain"), Usermeta: userMeta}
	_, err := riak.StoreObject("riakpbctestbucket", "testkey_rpbcontent", rpbObj)
	if err != nil {
		t.Error(err.Error())
	}

	obj, err := riak.FetchHead("riakpbctestbucket", "testkey_rpbcontent")
	oObj := obj.GetContent()
	assert.T(t, len(oObj) == 1)
	content := oObj[0]
	assert.T(t, len(content.GetValue()) == 0)
	assert.T(t, len(content.GetUsermeta()) == 1)
	assert.T(t, fmt.Sprintf("%s", content.GetUsermeta()[0]) == fmt.Sprintf("%s", &RpbPair{Key: []byte("meta"), Value: []byte("schmeta")}))
}

func TestStoreObject(t *testing.T) {
	riak := setupConnection(t)

	// Insert
	userMeta := []*RpbPair{&RpbPair{Key: []byte("meta"), Value: []byte("schmeta")}}
	rpbObj := &RpbContent{Value: []byte("rpbcontent data"), ContentType: []byte("text/plain"), Usermeta: userMeta}
	_, err := riak.StoreObject("riakpbctestbucket", "testkey_rpbcontent", rpbObj)
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
	/*
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
	*/
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
	result, err := riak.FetchStruct("riakpbctestbucket", "testkey_struct", data)
	if err != nil {
		t.Error(err.Error())
	}
	if len(result.GetContent()) != 1 {
		t.Error("expected FetchStruct to also return RpbGetResp content")
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
