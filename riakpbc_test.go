package riakpbc

import (
	"fmt"
	"github.com/bmizerany/assert"
	"log"
	"testing"
)

func ExampleConn() {
	riak := New([]string{"127.0.0.1:8087", "127.0.0.1:8088"})

	if err := riak.Dial(); err != nil {
		log.Print(err.Error())
	}

	data := []byte("{'data':'rules'}")

	_, err := riak.StoreObject("bucket", "data", data, "application/json", nil)
	if err != nil {
		log.Println(err.Error())
	}

	_, err = riak.SetClientId("coolio")
	if err != nil {
		log.Println(err.Error())
	}

	id, err := riak.GetClientId()
	if err != nil {
		log.Println(err.Error())
	}
	fmt.Println(string(id.GetClientId()))

	obj, err := riak.FetchObject("bucket", "data", nil)
	if err != nil {
		log.Println(err.Error())
	}
	fmt.Println(string(obj.GetContent()[0].GetValue()))
	// Output:
	// coolio
	// {'data':'rules'}

	riak.Close()
}

func setupConnection(t *testing.T) (conn *Conn) {
	conn = New([]string{"127.0.0.1:8087", "127.0.0.1:8088"})
	if err := conn.Dial(); err != nil {
		t.Error(err.Error())
	}
	assert.T(t, conn != nil)

	return conn
}

func setupData(t *testing.T, conn *Conn) {
	ok, err := conn.StoreObject("riakpbctestbucket", "testkey", []byte("{\"data\":\"is awesome!\"}"), "application/json", nil)
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, len(ok.GetKey()) == 0)
}

func teardownData(t *testing.T, conn *Conn) {
	ok, err := conn.DeleteObject("riakpbctestbucket", "testkey", nil)
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(ok) == "Success")
}

func TestFetchObject(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	object, err := riak.FetchObject("riakpbctestbucket", "testkey", nil)
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

func TestDeleteObject(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	object, err := riak.DeleteObject("riakpbctestbucket", "testkey", nil)
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(object) == "Success")

	_, err = riak.FetchObject("riakpbctestbucket", "testkey", nil)
	assert.T(t, err.Error() == "object not found")

	teardownData(t, riak)
}
