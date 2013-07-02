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

func setupConnection(t *testing.T) *Client {
	coder := NewCoder("json", JsonMarshaller, JsonUnmarshaller)
	client := NewClient([]string{"127.0.0.1:8087",
		"127.0.0.1:8088",
		"127.0.0.1:8087",
		"127.0.0.1:8088"}, coder)

	var err error
	if err = client.Dial(); err != nil {
		t.Error(err.Error())
	}
	assert.T(t, err == nil)

	return client
}

func setupSingleNodeConnection(t *testing.T) *Client {
	coder := NewCoder("json", JsonMarshaller, JsonUnmarshaller)
	client := NewClient([]string{"127.0.0.1:8087"}, coder)

	var err error
	if err = client.Dial(); err != nil {
		t.Error(err.Error())
	}
	assert.T(t, err == nil)

	return client
}

func setupData(t *testing.T, client *Client) {
	session := client.Session()
	ok, err := session.StoreObject("riakpbctestbucket", "testkey", "{\"data\":\"is awesome!\"}")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, len(ok.GetKey()) == 0)
	client.Free(session)
}

func teardownData(t *testing.T, client *Client) {
	session := client.Session()
	ok, err := session.DeleteObject("riakpbctestbucket", "testkey")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(ok) == "Success")
	client.Free(session)
}

func TestHead(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()
	userMeta := []*RpbPair{&RpbPair{Key: []byte("meta"), Value: []byte("schmeta")}}
	rpbObj := &RpbContent{Value: []byte("rpbcontent data"), ContentType: []byte("text/plain"), Usermeta: userMeta}
	_, err := session.StoreObject("riakpbctestbucket", "testkey_rpbcontent", rpbObj)
	if err != nil {
		t.Error(err.Error())
	}
	tB := new(bool)
	*tB = true
	opts := &RpbGetReq{
		Head: tB,
	}
	session.SetOpts(opts)
	obj, err := session.FetchObject("riakpbctestbucket", "testkey_rpbcontent")
	oObj := obj.GetContent()
	assert.T(t, len(oObj) == 1)
	content := oObj[0]
	assert.T(t, len(content.GetValue()) == 0)
	assert.T(t, len(content.GetUsermeta()) == 1)
	assert.T(t, fmt.Sprintf("%s", content.GetUsermeta()[0]) == fmt.Sprintf("%s", &RpbPair{Key: []byte("meta"), Value: []byte("schmeta")}))
	client.Free(session)
}

func TestStoreObject(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()

	// Insert
	_, err := session.StoreObject("riakpbctestbucket", "testkey_rpbcontent", &RpbContent{Value: []byte("rpbcontent data"), ContentType: []byte("text/plain")})
	if err != nil {
		t.Error(err.Error())
	}
	_, err = session.StoreObject("riakpbctestbucket", "testkey_string", "string data")
	if err != nil {
		t.Error(err.Error())
	}
	_, err = session.StoreObject("riakpbctestbucket", "testkey_int", 1000)
	if err != nil {
		t.Error(err.Error())
	}
	_, err = session.StoreObject("riakpbctestbucket", "testkey_binary", []byte("binary data"))
	if err != nil {
		t.Error(err.Error())
	}

	// Cleanup
	_, err = session.DeleteObject("riakpbctestbucket", "testkey_rpbcontent")
	if err != nil {
		t.Error(err.Error())
	}
	_, err = session.DeleteObject("riakpbctestbucket", "testkey_string")
	if err != nil {
		t.Error(err.Error())
	}
	_, err = session.DeleteObject("riakpbctestbucket", "testkey_int")
	if err != nil {
		t.Error(err.Error())
	}
	_, err = session.DeleteObject("riakpbctestbucket", "testkey_binary")
	if err != nil {
		t.Error(err.Error())
	}

	client.Free(session)
}

func TestStoreStruct(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()

	riak_data := &RiakData{
		Email:   "riak@example.com",
		Twitter: "riak-twitter",
		Data:    []byte("riak-data"),
	}

	_, err := session.StoreStruct("riakpbctestbucket", "testkey_struct", riak_data)
	if err != nil {
		t.Error(err.Error())
	}

	_, err = session.DeleteObject("riakpbctestbucket", "testkey_struct")
	if err != nil {
		t.Error(err.Error())
	}

	client.Free(session)
}

func TestStoreObjectWithOpts(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()

	data, err := json.Marshal(&Data{Data: "is awesome!"})
	if err != nil {
		log.Println(err.Error())
	}

	z := new(bool)
	*z = true
	opts := &RpbPutReq{
		ReturnBody: z,
	}
	session.SetOpts(opts)
	object, err := session.StoreStruct("riakpbctestbucket", "testkeyopts", &Data{Data: "is awesome!"})
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(object.GetContent()[0].GetValue()) == string(data))

	_, err = session.DeleteObject("riakpbctestbucket", "testkeyopts")
	if err != nil {
		t.Error(err.Error())
	}

	client.Free(session)
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
			session1 := client.Session()
			z := new(bool)
			*z = true
			opts := &RpbPutReq{
				ReturnBody: z,
			}
			session1.SetOpts(opts)
			object, err := session1.StoreStruct("riakpbctestbucket", "testkeyopts", &Data{Data: "is awesome!"})
			if err != nil {
				t.Error(err.Error())
			}
			assert.T(t, string(object.GetContent()[0].GetValue()) == string(data))
			client.Free(session1)

			session2 := client.Session()
			_, err = session2.StoreStruct("riakpbctestbucket", "testkeyopts", &Data{Data: "is awesome!"})
			if err != nil {
				t.Error(err.Error())
			}
			client.Free(session2)

			sym <- true
		}()
	}
	for i := 0; i < 10; i++ {
		<-sym
	}

}

func TestFetchObject(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()
	setupData(t, client)

	object, err := session.FetchObject("riakpbctestbucket", "testkey")
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
	client.Free(session)
}

func TestFetchStruct(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()
	setupData(t, client)

	riak_data := &RiakData{
		Email:   "riak@example.com",
		Twitter: "riak-twitter",
		Data:    []byte("riak-data"),
	}

	_, err := session.StoreStruct("riakpbctestbucket", "testkey_struct", riak_data)
	if err != nil {
		t.Error(err.Error())
	}

	// Test
	data := &RiakData{}
	result, err := session.FetchStruct("riakpbctestbucket", "testkey_struct", data)
	if err != nil {
		t.Error(err.Error())
	}
	if len(result.GetContent()) != 1 {
		t.Error("expected FetchStruct to also return RpbGetResp content")
	}

	_, err = session.DeleteObject("riakpbctestbucket", "testkey_struct")
	if err != nil {
		t.Error(err.Error())
	}

	teardownData(t, client)
	client.Free(session)
}

func TestDeleteObject(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()
	setupData(t, client)

	object, err := session.DeleteObject("riakpbctestbucket", "testkey")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(object) == "Success")

	_, err = session.FetchObject("riakpbctestbucket", "testkey")

	assert.T(t, err.Error() == "object not found")

	teardownData(t, client)
	client.Free(session)
}
