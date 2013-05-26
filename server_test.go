package riakpbc

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestServerInfo(t *testing.T) {
	riak := setupConnection(t)

	info, err := riak.GetServerInfo()
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(info.GetServerVersion()) != "")
}

func TestPing(t *testing.T) {
	riak := setupConnection(t)

	pong, err := riak.Ping()
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(pong) == "Pong")
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
	assert.T(t, string(clientId.GetClientId()) == "riakpbctestclientid")
}
