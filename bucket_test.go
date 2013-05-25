package riakpbc

import (
	"github.com/bmizerany/assert"
	"strings"
	"testing"
)

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
