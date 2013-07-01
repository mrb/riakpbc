package riakpbc

import (
	"encoding/json"
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

func setupConnection(t *testing.T) *Client {
	client := NewClient([]string{"127.0.0.1:8087",
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

func setupSingleNodeConnection(t *testing.T) *Client {
	client := NewClient([]string{"127.0.0.1:8087"})
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
	node := client.Session()
	ok, err := node.StoreObject("riakpbctestbucket", "testkey", "{\"data\":\"is awesome!\"}")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, len(ok.GetKey()) == 0)
}

func teardownData(t *testing.T, client *Client) {
	node := client.Session()
	ok, err := node.DeleteObject("riakpbctestbucket", "testkey")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(ok) == "Success")
}

func TestStoreObject(t *testing.T) {
	client := setupConnection(t)
	riak := client.Session()

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
	client := setupConnection(t)
	riak := client.Session()

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
	client := setupConnection(t)
	riak := client.Session()

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

func TestConcurrentOpts(t *testing.T) {
	client := setupConnection(t)

	data, err := json.Marshal(&Data{Data: "is awesome!"})
	if err != nil {
		log.Println(err.Error())
	}

	sym := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			riak1 := client.Session()
			z := new(bool)
			*z = true
			opts := &RpbPutReq{
				ReturnBody: z,
			}
			riak1.SetOpts(opts)
			object, err := riak1.StoreStruct("riakpbctestbucket", "testkeyopts", &Data{Data: "is awesome!"})
			if err != nil {
				t.Error(err.Error())
			}
			assert.T(t, string(object.GetContent()[0].GetValue()) == string(data))

			riak2 := client.Session()
			_, err = riak2.StoreStruct("riakpbctestbucket", "testkeyopts", &Data{Data: "is awesome!"})
			if err != nil {
				t.Error(err.Error())
			}
			sym <- true
		}()
	}
	for i := 0; i < 10; i++ {
		<-sym
	}

}

func TestFetchObject(t *testing.T) {
	client := setupConnection(t)
	riak := client.Session()
	setupData(t, client)

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

	teardownData(t, client)
}

func TestFetchStruct(t *testing.T) {
	client := setupConnection(t)
	riak := client.Session()
	setupData(t, client)

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

	teardownData(t, client)
}

func TestDeleteObject(t *testing.T) {
	client := setupConnection(t)
	riak := client.Session()
	setupData(t, client)

	object, err := riak.DeleteObject("riakpbctestbucket", "testkey")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(object) == "Success")

	_, err = riak.FetchObject("riakpbctestbucket", "testkey")

	assert.T(t, err.Error() == "object not found")

	teardownData(t, client)
}
