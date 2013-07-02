package riakpbc

import (
	"testing"
)

func TestLink(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()
	setupData(t, client)

	if _, err := session.StoreObject("riakpbclinktestbucket1", "linktestkeyb1k1", "link start data"); err != nil {
		t.Error(err.Error())
	}

	if _, err := session.StoreObject("riakpbclinktestbucket2", "linktestkeyb2k1", "link next data"); err != nil {
		t.Error(err.Error())
	}

	if err := session.LinkAdd("riakpbclinktestbucket1", "linktestkeyb1k1", "riakpbclinktestbucket2", "linktestkeyb2k1", "riaklinktag"); err != nil {
		t.Error(err.Error())
	}

	obj, err := session.FetchObject("riakpbclinktestbucket1", "linktestkeyb1k1")
	if err != nil {
		t.Error(err.Error())
	}

	if string(obj.GetContent()[0].GetLinks()[0].GetBucket()) != "riakpbclinktestbucket2" {
		t.Error("expected link to riakpbclinktestbucket2")
	}

	link, err := session.LinkWalk(string(obj.GetContent()[0].GetLinks()[0].GetBucket()), string(obj.GetContent()[0].GetLinks()[0].GetKey()))
	if err != nil {
		t.Error(err.Error())
	}

	if string(link.GetContent()[0].GetValue()) != "link next data" {
		t.Error("expected link walk to result in 'link next data'")
	}

	if err := session.LinkRemove("riakpbclinktestbucket1", "linktestkeyb1k1", "riakpbclinktestbucket2", "linktestkeyb2k1"); err != nil {
		t.Error(err.Error())
	}

	check, err := session.FetchObject("riakpbclinktestbucket1", "linktestkeyb1k1")
	if err != nil {
		t.Error(err.Error())
	}

	if len(check.GetContent()[0].GetLinks()) > 0 {
		t.Error("expected links to be empty")
	}

	teardownData(t, client)
	client.Free(session)
}
