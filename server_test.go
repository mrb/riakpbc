package riakpbc

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestServerInfo(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()

	info, err := session.GetServerInfo()
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, info != nil)
	assert.T(t, string(info.GetServerVersion()) != "")

	client.Free(session)
}

func TestPing(t *testing.T) {
	client := setupConnection(t)
	session := client.Session()

	pong, err := session.Ping()
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(pong) == "Pong")

	client.Free(session)
}

func TestClientId(t *testing.T) {
	client := setupSingleNodeConnection(t)
	session := client.Session()
	ok, err := session.SetClientId("riakpbctestclientid")
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(ok) == "Success")

	clientId, err := session.GetClientId()
	if err != nil {
		t.Error(err.Error())
	}
	assert.T(t, string(clientId.GetClientId()) != "")

	client.Free(session)
}
