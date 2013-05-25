package riakpbc

import (
	"fmt"
	"github.com/bmizerany/assert"
	"strings"
	"testing"
)

func TestListBuckets(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	buckets, err := riak.ListBuckets()
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, strings.Contains(buckets.String(), "riakpbctestbucket"))

	teardownData(t, riak)
}

func TestListKeys(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	keys, err := riak.ListKeys("riakpbctestbucket")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, strings.Contains(fmt.Sprintf("%s", keys), "testkey"))

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
	assert.T(t, strings.Contains(string(bucket.GetProps().String()), "false"))

	teardownData(t, riak)
}
