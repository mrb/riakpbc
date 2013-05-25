/*
Package json is a simple API wrapper for handling JSON data in riakpbc.
*/
package json

import (
	"encoding/json"
	"errors"
	"github.com/mrb/riakpbc"
)

var (
	ErrNoBucketSet = errors.New("no bucket set")
	ErrNoKeySet    = errors.New("no key set")
)

type JsonWrapper struct {
	conn   *riakpbc.Conn // wrapped connection
	bucket string        // currently set bucket
	key    string        // current key being searched on
}

func NewJsonWrapper(conn *riakpbc.Conn) *JsonWrapper {
	w := new(JsonWrapper)
	w.conn = conn
	return w
}

// Bucket sets the current Riak bucket to use.
func (self *JsonWrapper) Bucket(bucket string) *JsonWrapper {
	self.bucket = bucket
	return self
}

// Find is meant to be chained with One() or All().
// Returns any matches to key.
func (self *JsonWrapper) Find(key string) *JsonWrapper {
	self.key = key
	return self
}

// One returns a single record into the passed struct.
func (self *JsonWrapper) One(passed interface{}) (err error) {
	if self.bucket == "" {
		return ErrNoBucketSet
	}

	if self.key == "" {
		return ErrNoKeySet
	}

	content, err := self.conn.FetchObject(self.bucket, self.key)
	if err != nil {
		return
	}
	return json.Unmarshal(content, &passed)
}

// Insert stores the passed struct into the bucket with key.
func (self *JsonWrapper) Insert(key string, passed interface{}) (err error) {
	if self.bucket == "" {
		return ErrNoBucketSet
	}

	content, err := json.Marshal(&passed)
	if err != nil {
		return
	}
	_, err = self.conn.StoreObject(self.bucket, key, content, "application/json")
	return
}

// Update is a convenience name JsonWrapper for Insert.  The two actions are the same.
func (self *JsonWrapper) Update(key string, passed interface{}) error {
	return self.Insert(key, passed)
}

// Delete removes the object from the bucket with key.
func (self *JsonWrapper) Delete(key string) (err error) {
	if self.bucket == "" {
		return ErrNoBucketSet
	}

	_, err = self.conn.DeleteObject(self.bucket, key)
	return
}
