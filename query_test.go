package riakpbc

import (
	"encoding/json"
	"github.com/bmizerany/assert"
	"os/exec"
	"testing"
)

type Farm struct {
	Animal string `json:"animal"`
}

func setupIndexing(t *testing.T) {
	cmd := exec.Command("search-cmd", "install", "riakpbctestbucket")
	err := cmd.Run()
	if err != nil {
		t.Error(err.Error())
	}
}

func teardownIndexing(t *testing.T) {
	cmd := exec.Command("search-cmd", "uninstall", "riakpbctestbucket")
	err := cmd.Run()
	if err != nil {
		t.Error(err.Error())
	}
}

func TestMapReduce(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	twoLevelQuery := "{\"inputs\":[[\"riakpbctestbucket\",\"testkey\"]],\"query\":[{\"map\":{\"language\":\"javascript\",\"keep\":false,\"name\":\"Riak.mapValuesJson\"}},{\"reduce\":{\"language\":\"javascript\",\"keep\":true,\"name\":\"Riak.reduceMax\"}}]}"
	reduced, err := riak.MapReduce(twoLevelQuery, "application/json")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(reduced) == "[{\"data\":\"is awesome!\"}]")

	teardownData(t, riak)
}

func TestIndex(t *testing.T) {
	riak := setupConnection(t)
	d1, err := json.Marshal(&Farm{Animal: "chicken"})
	if err != nil {
		t.Error(err.Error())
	}
	i1 := &RpbPair{
		Key:   []byte("animal_bin"),
		Value: []byte("chicken"),
	}
	c1 := &RpbContent{
		Value:       d1,
		ContentType: []byte("application/json"),
		Indexes: []*RpbPair{
			i1,
		},
	}
	if _, err := riak.StoreObject("farm", "chicken", c1); err != nil {
		t.Error(err.Error())
	}

	d2, err := json.Marshal(&Farm{Animal: "hen"})
	if err != nil {
		t.Error(err.Error())
	}
	i2 := &RpbPair{
		Key:   []byte("animal_bin"),
		Value: []byte("chicken"),
	}
	c2 := &RpbContent{
		Value:       d2,
		ContentType: []byte("application/json"),
		Indexes: []*RpbPair{
			i2,
		},
	}
	if _, err := riak.StoreObject("farm", "hen", c2); err != nil {
		t.Error(err.Error())
	}

	d3, err := json.Marshal(&Farm{Animal: "rooster"})
	if err != nil {
		t.Error(err.Error())
	}
	i3 := &RpbPair{
		Key:   []byte("animal_bin"),
		Value: []byte("chicken"),
	}
	c3 := &RpbContent{
		Value:       d3,
		ContentType: []byte("application/json"),
		Indexes: []*RpbPair{
			i3,
		},
	}
	if _, err := riak.StoreObject("farm", "rooster", c3); err != nil {
		t.Error(err.Error())
	}

	data, err := riak.Index("farm", "animal_bin", "chicken", "", "")
	if err != nil {
		t.Log("In order for this test to pass storage_backend must be set to riak_kv_eleveldb_backend in app.config")
		t.Error(err.Error())
	}
	assert.T(t, len(data.GetKeys()) > 0)

	if _, err := riak.DeleteObject("farm", "chicken"); err != nil {
		t.Error(err.Error())
	}
	if _, err := riak.DeleteObject("farm", "hen"); err != nil {
		t.Error(err.Error())
	}
	if _, err := riak.DeleteObject("farm", "rooster"); err != nil {
		t.Error(err.Error())
	}
}

func TestSearch(t *testing.T) {
	riak := setupConnection(t)
	setupIndexing(t)
	setupData(t, riak)

	data, err := riak.Search("*awesome*", "data")
	if err != nil {
		t.Log("In order for this test to pass riak_search may need to be enabled in app.config")
		t.Error(err.Error())
	}
	assert.T(t, data.GetNumFound() > 0)

	teardownData(t, riak)
	teardownIndexing(t)
}
