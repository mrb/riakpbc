package riakpbc

import (
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

func TestClientId(t *testing.T) {
	riak := setupConnection(t)

	ok, err := riak.SetClientId("testriakpbc")
	assert.T(t, err == nil)
	assert.T(t, string(ok) == "Success")

	clientId, err := riak.GetClientId()
	assert.T(t, err == nil)
	assert.T(t, string(clientId) == "testriakpbc")
}

func TestListBuckets(t *testing.T) {
	riak := setupConnection(t)
	buckets, _ := riak.ListBuckets()
	bucketString := fmt.Sprintf("%s", buckets)
	log.Print(bucketString)
	assert.T(t, strings.Contains(bucketString, "riakpbctestbucket"))
}

func TestGetAndSetBuckets(t *testing.T) {
	riak := setupConnection(t)

	nval := uint32(1)
	allowmult := false
	ok, err := riak.SetBucket("riakpbctestbucket", &nval, &allowmult)
	assert.T(t, err == nil)
	assert.T(t, string(ok) == "Success")

	bucket, err := riak.GetBucket("riakpbctestbucket")
	assert.T(t, err == nil)
	assert.T(t, strings.Contains(string(bucket), "true"))
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
