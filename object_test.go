package riakpbc

import (
	"fmt"
	"github.com/bmizerany/assert"
	"os"
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
	coder := NewCoder("json", JsonMarshaller, JsonUnmarshaller)
	client = NewClientWithCoder([]string{
		"127.0.0.1:8086",
		"127.0.0.1:8087",
		"127.0.0.1:8088",
		"127.0.0.1:8089"},
		coder)
	client.EnableLogging()
	var err error
	if err = client.Dial(); err != nil {
		os.Exit(1)
		t.Error(err.Error())
	}
	assert.T(t, err == nil)

	return client
}

func setupSingleNodeConnection(t *testing.T) (client *Client) {
	coder := NewCoder("json", JsonMarshaller, JsonUnmarshaller)
	client = NewClientWithCoder([]string{"127.0.0.1:8087"}, coder)
	var err error
	if err = client.Dial(); err != nil {
		os.Exit(1)
		t.Error(err.Error())
	}
	assert.T(t, err == nil)

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
	tB := new(bool)
	*tB = true
	opts := riak.NewFetchObjectRequest("riakpbctestbucket", "testkey_rpbcontent")
	opts.Head = tB
	obj, err := riak.Do(opts)
	oObj := obj.(*RpbGetResp).GetContent()
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

func TestStoreObjectDo(t *testing.T) {
	riak := setupConnection(t)

	// Insert
	userMeta := []*RpbPair{&RpbPair{Key: []byte("meta"), Value: []byte("schmeta")}}
	rpbObj := &RpbContent{Value: []byte("rpbcontent data"), ContentType: []byte("text/plain"), Usermeta: userMeta}

	opts := riak.NewStoreObjectRequest("riakpbctestbucket", "testkey_rpbcontent")
	_, err := riak.DoObject(opts, rpbObj)
	if err != nil {
		t.Error(err.Error())
	}
	opts = riak.NewStoreObjectRequest("riakpbctestbucket", "testkey_string")
	_, err = riak.DoObject(opts, "string data")
	if err != nil {
		t.Error(err.Error())
	}
	opts = riak.NewStoreObjectRequest("riakpbctestbucket", "testkey_int")
	_, err = riak.DoObject(opts, 1000)
	if err != nil {
		t.Error(err.Error())
	}
	opts = riak.NewStoreObjectRequest("riakpbctestbucket", "testkey_binary")
	_, err = riak.DoObject(opts, []byte("binary data"))
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

func TestStoreStructDo(t *testing.T) {
	riak := setupConnection(t)

	riak_data := &RiakData{
		Email:   "riak@example.com",
		Twitter: "riak-twitter",
		Data:    []byte("riak-data"),
	}

	opts := riak.NewStoreStructRequest("riakpbctestbucket", "testkey_struct")
	_, err := riak.DoStruct(opts, riak_data)
	if err != nil {
		t.Error(err.Error())
	}

	_, err = riak.DeleteObject("riakpbctestbucket", "testkey_struct")
	if err != nil {
		t.Error(err.Error())
	}
}

func TestStoreStructFullTypes(t *testing.T) {
	riak := setupConnection(t)

	// Test struct from coder.go
	data := EncodeData{
		Email:   "riak@example.com",
		Twitter: "riak-twitter",
		Data:    []byte("riak-data"),
		AnInt:   1,
		AnInt8:  127,
		AnInt16: 32767,
		AnInt32: 2147483647,
		AnInt64: 9223372036854775807,
		AUInt:   1,
		AUInt8:  255,
		AUInt16: 65535,
		AUInt32: 4294967295,
		AUInt64: 18446744073709551615,
		Byte:    255,
		Rune:    2147483647,
	}

	_, err := riak.StoreStruct("riakpbctestbucket", "testfulltypes", &data)
	if err != nil {
		t.Error(err.Error())
	}

	_, err = riak.DeleteObject("riakpbctestbucket", "testfulltypes")
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
	} else {

		stringObject := string(object.GetContent()[0].GetValue())

		data := "{\"data\":\"is awesome!\"}"
		if err != nil {
			t.Error(err.Error())
		}
		assert.T(t, stringObject == data)

	}

	teardownData(t, riak)
}

func TestFetchObjectDo(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	opts := riak.NewFetchObjectRequest("riakpbctestbucket", "testkey")
	object, err := riak.Do(opts)
	if err != nil {
		t.Error(err.Error())
	} else {
		stringObject := string(object.(*RpbGetResp).GetContent()[0].GetValue())

		data := "{\"data\":\"is awesome!\"}"
		if err != nil {
			t.Error(err.Error())
		}
		assert.T(t, stringObject == data)

	}

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

func TestFetchStructDo(t *testing.T) {
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
	opts := riak.NewFetchStructRequest("riakpbctestbucket", "testkey_struct")
	result, err := riak.DoStruct(opts, data)
	if err != nil {
		t.Error(err.Error())
	}
	if len(result.(*RpbGetResp).GetContent()) != 1 {
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

func TestDeleteObjectDo(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	opts := riak.NewDeleteObjectRequest("riakpbctestbucket", "testkey")
	object, err := riak.Do(opts)
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(string(object.([]byte))) == "Success")

	_, err = riak.FetchObject("riakpbctestbucket", "testkey")

	assert.T(t, err.Error() == "object not found")

	teardownData(t, riak)
}
