package riakpbc

import (
	"testing"
)

func TestLink(t *testing.T) {
	riak := setupConnection(t)
	setupData(t, riak)

	if _, err := riak.StoreObject("riakpbclinktestbucket1", "linktestkeyb1k1", "link start data"); err != nil {
		t.Error(err.Error())
	}

	if _, err := riak.StoreObject("riakpbclinktestbucket2", "linktestkeyb2k1", "link next data"); err != nil {
		t.Error(err.Error())
	}

	if err := riak.LinkAdd("riakpbclinktestbucket1", "linktestkeyb1k1", "riakpbclinktestbucket2", "linktestkeyb2k1", "riaklinktag"); err != nil {
		t.Error(err.Error())
	}

	obj, err := riak.FetchObject("riakpbclinktestbucket1", "linktestkeyb1k1")
	if err != nil {
		t.Error(err.Error())
	}

	if obj == nil {
		t.Error("data corrupt")
	} else {

		if string(obj.GetContent()[0].GetLinks()[0].GetBucket()) != "riakpbclinktestbucket2" {
			t.Error("expected link to riakpbclinktestbucket2")
		}

		link, err := riak.LinkWalk(string(obj.GetContent()[0].GetLinks()[0].GetBucket()), string(obj.GetContent()[0].GetLinks()[0].GetKey()))
		if err != nil {
			t.Error(err.Error())
		}

		if string(link.GetContent()[0].GetValue()) != "link next data" {
			t.Error("expected link walk to result in 'link next data'")
		}

		if err := riak.LinkRemove("riakpbclinktestbucket1", "linktestkeyb1k1", "riakpbclinktestbucket2", "linktestkeyb2k1"); err != nil {
			t.Error(err.Error())
		}

		check, err := riak.FetchObject("riakpbclinktestbucket1", "linktestkeyb1k1")
		if err != nil {
			t.Error(err.Error())
		}

		if len(check.GetContent()[0].GetLinks()) > 0 {
			t.Error("expected links to be empty")
		}
	}

	teardownData(t, riak)
}
