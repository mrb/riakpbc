package riakpbc

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestServerInfo(t *testing.T) {
	client := setupConnection(t)
	riak := client.Session()

	info, err := riak.GetServerInfo()
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, info != nil)
	assert.T(t, string(info.GetServerVersion()) != "")
}

func TestPing(t *testing.T) {
	client := setupConnection(t)
	riak := client.Session()

	pong, err := riak.Ping()
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(pong) == "Pong")
}

func TestClientId(t *testing.T) {
	client := setupSingleNodeConnection(t)
	riak := client.Session()
	ok, err := riak.SetClientId("riakpbctestclientid")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(ok) == "Success")

	clientId, err := riak.GetClientId()
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(clientId.GetClientId()) != "")
}
