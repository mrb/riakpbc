package riakpbc

import (
	"errors"
	"github.com/bmizerany/assert"
	"testing"
)

type FakeRetrier struct{}

func (retrier *FakeRetrier) Read(readfunc NetworkReadFunc) ([]byte, error) {
	return []byte{}, errors.New("FAKE RETRIER READ")
}

func (retrier *FakeRetrier) Write(writefunc NetworkWriteFunc, data []byte) error {
	return errors.New("FAKE RETRIER WRITE")
}

func TestNewRetryPolicy(t *testing.T) {
	riak := setupSingleNodeConnection(t)
	riak.SetRetryPolicy(&FakeRetrier{})

	object, err := riak.FetchObject("riakpbctestbucket", "testkey")
	assert.Equal(t, object, (*RpbGetResp)(nil))
	assert.Equal(t, err.Error(), "FAKE RETRIER WRITE")
}
