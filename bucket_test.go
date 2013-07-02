package riakpbc

import (
	"fmt"
	"github.com/bmizerany/assert"
	"strings"
	"testing"
)

func TestListBuckets(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()
	setupData(t, client)

	buckets, err := session.ListBuckets()
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, strings.Contains(buckets.String(), "riakpbctestbucket"))

	teardownData(t, client)
	client.Free(session)
}

func TestListKeys(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()
	setupData(t, client)

	keys, err := session.ListKeys("riakpbctestbucket")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, strings.Contains(fmt.Sprintf("%s", keys), "testkey"))

	teardownData(t, client)
	client.Free(session)
}

func TestGetAndSetBuckets(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()
	setupData(t, client)

	nval := uint32(1)
	allowmult := false
	ok, err := session.SetBucket("riakpbctestbucket", &nval, &allowmult)
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(ok) == "Success")

	bucket, err := session.GetBucket("riakpbctestbucket")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, strings.Contains(string(bucket.GetProps().String()), "false"))

	teardownData(t, client)
	client.Free(session)
}
