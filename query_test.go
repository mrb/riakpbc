package riakpbc

import (
	"github.com/bmizerany/assert"
	"os/exec"
	"testing"
)

type Farm struct {
	Animal string `json:"animal" riak:"index"`
}

func setupIndexing(t *testing.T) {
	_, err := exec.Command("curl", "-i", "-XPUT", "http://127.0.0.1:10018/riak/farm", "-H", "Content-Type: application/json", "-d", "{\"props\":{\"search\":true}}").Output()
	if err != nil {
		t.Error(err.Error())
	}
}

func TestMapReduce(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()
	setupData(t, client)

	twoLevelQuery := "{\"inputs\":[[\"riakpbctestbucket\",\"testkey\"]],\"query\":[{\"map\":{\"language\":\"javascript\",\"keep\":false,\"name\":\"Riak.mapValuesJson\"}},{\"reduce\":{\"language\":\"javascript\",\"keep\":true,\"name\":\"Riak.reduceMax\"}}]}"
	reduced, err := session.MapReduce(twoLevelQuery, "application/json")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(reduced) == "[{\"data\":\"is awesome!\"}]")

	teardownData(t, client)
	client.Free(session)
}

func TestIndex(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()
	if _, err := session.StoreStruct("farm", "chicken", &Farm{Animal: "chicken"}); err != nil {
		t.Error(err.Error())
	}
	if _, err := session.StoreStruct("farm", "hen", &Farm{Animal: "hen"}); err != nil {
		t.Error(err.Error())
	}
	if _, err := session.StoreStruct("farm", "rooster", &Farm{Animal: "rooster"}); err != nil {
		t.Error(err.Error())
	}

	data, err := session.Index("farm", "animal_bin", "chicken", "", "")
	if err != nil {
		t.Log("In order for this test to pass storage_backend must be set to riak_kv_eleveldb_backend in app.config")
		t.Error(err.Error())
	}
	assert.T(t, len(data.GetKeys()) > 0)

	if _, err := session.DeleteObject("farm", "chicken"); err != nil {
		t.Error(err.Error())
	}
	if _, err := session.DeleteObject("farm", "hen"); err != nil {
		t.Error(err.Error())
	}
	if _, err := session.DeleteObject("farm", "rooster"); err != nil {
		t.Error(err.Error())
	}

	// Search against a non-existent key should return empty, not error
	check, err := session.Index("farm", "animal_bin", "chicken", "", "")
	if len(check.GetKeys()) > 0 {
		t.Error("non-existent index search should return 0 results")
	}
	client.Free(session)
}

func TestSearch(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()
	setupIndexing(t)
	if _, err := session.StoreStruct("farm", "chicken", &Farm{Animal: "chicken"}); err != nil {
		t.Error(err.Error())
	}

	data, err := session.Search("farm", "animal:chicken")
	if err != nil {
		t.Log("In order for this test to pass riak_search may need to be enabled in app.config")
		t.Error(err.Error())
	}
	assert.T(t, data.GetNumFound() > 0)

	if _, err := session.DeleteObject("farm", "chicken"); err != nil {
		t.Error(err.Error())
	}
	client.Free(session)
}
