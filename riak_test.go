package riakpbc

import (
	"encoding/json"
	"fmt"
	"github.com/bmizerany/assert"
	"log"
	"mrb/riakpbc"
	"strings"
	"testing"
)

func setupConnection(t *testing.T) (conn *riakpbc.Conn) {
	conn, err := riakpbc.Dial("127.0.0.1:8087")
	assert.T(t, err == nil)
	assert.T(t, conn != nil)

	return
}

func setupData(t *testing.T, conn *riakpbc.Conn) {
	ok, err := conn.StoreObject("riakpbctestbucket", "testkey", "{\"data\":\"is awesome!\"}")
	assert.T(t, err == nil)
	assert.T(t, string(ok) == "Success")
}

func teardownData(t *testing.T, conn *riakpbc.Conn) {
	ok, err := conn.DeleteObject("riakpbctestbucket", "testkey")
	assert.T(t, err == nil)
	assert.T(t, string(ok) == "Success")
}

func TestClientId(t *testing.T) {
	riak := setupConnection(t)

	ok, err := riak.SetClientId("riakpbctestclientid")
	assert.T(t, err == nil)
	assert.T(t, string(ok) == "Success")

	clientId, err := riak.GetClientId()
	assert.T(t, err == nil)
	assert.T(t, string(clientId) == "riakpbctestclientid")
}

func TestListBuckets(t *testing.T) {
	riak := setupConnection(t)

	setupData(t, riak)

	buckets, err := riak.ListBuckets()
	assert.T(t, err == nil)

	bucketString := fmt.Sprintf("%s", buckets)
	assert.T(t, strings.Contains(bucketString, "riakpbctestbucket"))

	teardownData(t, riak)
}

func TestFetchObject(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	object, err := riak.FetchObject("riakpbctestbucket", "testkey")
	assert.T(t, err == nil)
	stringObject := string(object)

	jsonD, err := json.Marshal("{\"data\":\"is awesome!\"}")
	assert.T(t, err == nil)
	assert.T(t, stringObject == string(jsonD))

	teardownData(t, riak)
}

func TestDeleteObject(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	object, err := riak.DeleteObject("riakpbctestbucket", "testkey")
	assert.T(t, err == nil)
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
	assert.T(t, err == nil)
	assert.T(t, string(ok) == "Success")

	bucket, err := riak.GetBucket("riakpbctestbucket")
	assert.T(t, err == nil)
	assert.T(t, strings.Contains(string(bucket), "false"))

	teardownData(t, riak)
}

func TestPing(t *testing.T) {
	riak := setupConnection(t)

	pong, err := riak.Ping()
	assert.T(t, string(pong) == "Pong")
	assert.T(t, err == nil)
}

func TestMapReduce(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	twoLevelQuery := "{\"inputs\":[[\"riakpbctestbucket\",\"testkey\"]],\"query\":[{\"map\":{\"language\":\"javascript\",\"keep\":false,\"name\":\"Riak.mapValuesJson\"}},{\"reduce\":{\"language\":\"javascript\",\"keep\":true,\"name\":\"Riak.reduceMax\"}}]}"
	reduced, err := riak.MapReduce(twoLevelQuery)
	assert.T(t, err == nil)
	assert.T(t, reduced != nil)
	assert.T(t, len(reduced) == 2)

	teardownData(t, riak)
}

func BenchmarkRead(b *testing.B) {
	b.N = 1000
	riak, err := riakpbc.Dial("127.0.0.1:8087")

	if err != nil {
		log.Print(err)
		return
	}

	for i := 0; i < b.N; i++ {
		_, _ = riak.FetchObject("bucket", "keyzles")
	}
}
