package riakpbc

import (
	"fmt"
	"github.com/bmizerany/assert"
	"strings"
	"testing"
	"flag"
)

var (
	backEndAddress = flag.String("backend_address", "127.0.0.1:10017", "Storage backend address")
)

func init(){
	flag.Parse()
}

func setupConnection(t *testing.T) (conn *Conn) {
	conn, err := New([]string{*backEndAddress}, 1e8, 1e8)
	conn.Dial()

	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, conn != nil)

	return conn
}

func setupData(t *testing.T, conn *Conn) {
	ok, err := conn.StoreObject("riakpbctestbucket", "testkey", []byte("{\"data\":\"is awesome!\"}"), "application/json")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(ok) == "Success")
}

func teardownData(t *testing.T, conn *Conn) {
	ok, err := conn.DeleteObject("riakpbctestbucket", "testkey")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(ok) == "Success")
}

func TestClientId(t *testing.T) {
	riak := setupConnection(t)
	ok, err := riak.SetClientId("riakpbctestclientid")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(ok) == "Success")

	clientId, err := riak.GetClientId()
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(clientId) == "riakpbctestclientid")
}

func TestListBuckets(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	buckets, err := riak.ListBuckets()
	if err != nil {
		t.Error(err.Error())
	}

	bucketString := fmt.Sprintf("%s", buckets)
	assert.T(t, strings.Contains(bucketString, "riakpbctestbucket"))

	teardownData(t, riak)
}

func TestFetchObject(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	object, err := riak.FetchObject("riakpbctestbucket", "testkey")
	if err != nil {
		t.Error(err.Error())
	}
	stringObject := string(object)

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

	object, err := riak.DeleteObject("riakpbctestbucket", "testkey")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(object) == "Success")

	object, err = riak.FetchObject("riakpbctestbucket", "testkey")
	assert.T(t, err.Error() == "object not found")

	teardownData(t, riak)
}

func TestGetAndSetBuckets(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	nval := uint32(1)
	allowmult := false
	ok, err := riak.SetBucket("riakpbctestbucket", &nval, &allowmult)
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(ok) == "Success")

	bucket, err := riak.GetBucket("riakpbctestbucket")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, strings.Contains(string(bucket), "false"))

	teardownData(t, riak)
}

func TestPing(t *testing.T) {
	riak := setupConnection(t)

	pong, err := riak.Ping()
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(pong) == "Pong")
}
