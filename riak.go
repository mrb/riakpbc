package riakpbc

import (
	"encoding/json"
)

// Store an object in riak
func (c *Conn) StoreObject(bucket string, key string, content string) (response []byte, err error) {
	jval, err := json.Marshal(content)

	reqstruct := &RpbPutReq{
		Bucket: []byte(bucket),
		Key:    []byte(key),
		Content: &RpbContent{
			Value:       []byte(jval),
			ContentType: []byte("application/json"),
		},
	}

	err = c.Request(reqstruct, "RpbPutReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbGetResp{}, "RpbPutResp")
	if err != nil {
		return nil, err
	}

	response = uncoercedresponse.([]byte)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// Fetch an object from a bucket
func (c *Conn) FetchObject(bucket string, key string) (response []byte, err error) {
	reqstruct := &RpbGetReq{
		Bucket: []byte(bucket),
		Key:    []byte(key),
	}

	err = c.Request(reqstruct, "RpbGetReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbGetResp{}, "RpbGetResp")
	if err != nil {
		return nil, err
	}

	response = uncoercedresponse.([]byte)
	if err != nil {
		return nil, err
	}

	return response, nil
}

/*
// List all keys from bucket
func (c *Conn) ListKeys(bucket string) (response [][]byte, err error) {
	reqstruct := &RpbListKeysReq{
		Bucket: []byte(bucke,
	}

	return response, nil
}
*/

// Get server info
func (c *Conn) GetServerInfo() (response []byte, err error) {
	reqdata := []byte{0, 0, 0, 1, 7}

	err = c.Request(reqdata, "RpbGetServerInfoReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbGetServerInfoResp{}, "RpbGetServerInfoResp")

	if err != nil {
		return nil, err
	}

	response = uncoercedresponse.([]byte)

	return response, nil
}

// Get bucket info
func (c *Conn) GetBucket(bucket string) (response []byte, err error) {

	return response, nil
}

// MapReduce
func (c *Conn) MapReduce(content string) (response [][]byte, err error) {

	reqstruct := &RpbMapRedReq{
		Request:     []byte(content),
		ContentType: []byte("application/json"),
	}

	err = c.Request(reqstruct, "RpbMapRedReq")
	if err != nil {
		return nil, err
	}

	var rawresp interface{}
	rawresp, err = c.Response(&RpbMapRedResp{}, "RpbMapRedResp")
	if err != nil {
		return nil, err
	}

	done := rawresp.(*RpbMapRedResp).Done
	respresp := rawresp.(*RpbMapRedResp).Response

	response = append(response, respresp)

	for done == nil {
		moreresp, moreerr := c.Response(&RpbMapRedResp{}, "RpbMapRedResp")
		if moreerr != nil {
			return nil, moreerr
		}

		done = moreresp.(*RpbMapRedResp).Done
		response = append(response, moreresp.(*RpbMapRedResp).Response)
	}

	return response, nil
}

// Create bucket
func (c *Conn) SetBucket(bucket string, nval *uint32, allowmult *bool) (response []byte, err error) {
	propstruct := &RpbBucketProps{
		NVal:      nval,
		AllowMult: allowmult,
	}

	reqstruct := &RpbSetBucketReq{
		Bucket: []byte(bucket),
		Props:  propstruct,
	}

	err = c.Request(reqstruct, "RpbSetBucketReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbSetBucketResp{}, "RpbSetBucketResp")
	if err != nil {
		return nil, err
	}

	response = uncoercedresponse.([]byte)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// List all buckets
func (c *Conn) ListBuckets() (response [][]byte, err error) {
	reqdata := []byte{0, 0, 0, 1, 15}

	err = c.Request(reqdata, "RpbListBucketsReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbListBucketsResp{}, "RpbListBucketsResp")

	response = uncoercedresponse.([][]byte)
	if err != nil {
		return nil, err
	}

	return response, nil
}
