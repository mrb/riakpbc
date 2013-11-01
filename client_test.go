package riakpbc

import (
	"github.com/bmizerany/assert"
	"testing"
)

func clientTestSetupSingleNodeConnection(t *testing.T) (client *Client) {
	client = NewClient([]string{"127.0.0.1:8087"})
	var err error
	if err = client.Dial(); err != nil {
		t.Fatal(err)
	}
	assert.T(t, err == nil)

	return client
}

func TestBackgroundPingDoesNotCausePanicOnClose(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			t.Fatal(err)
		}
	}()

	riak := clientTestSetupSingleNodeConnection(t)
	riak.Close()
	riak.BackgroundNodePing()
}
