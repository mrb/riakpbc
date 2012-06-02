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

	uncoercedresponse, err := c.Response(&RpbGetResp{})
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
	c.mu.Lock()
	defer c.mu.Unlock()

	reqstruct := &RpbGetReq{
		Bucket: []byte(bucket),
		Key:    []byte(key),
	}

	err = c.Request(reqstruct, "RpbGetReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbGetResp{})
	if err != nil {
		return nil, err
	}

	response = uncoercedresponse.([]byte)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// List all keys from bucket
func (c *Conn) ListKeys(bucket string) (response [][]byte, err error) {
	reqstruct := &RpbListKeysReq{
		Bucket: []byte(bucket),
	}

	err = c.Request(reqstruct, "RpbListKeysReq")
	if err != nil {
		return nil, err
	}

	var rawresp interface{}
	rawresp, err = c.Response(&RpbListKeysResp{})
	if err != nil {
		if err != ErrReadTimeout {
			return nil, err
		}
	}

	if rawresp == nil {
		err = ErrNotDone
		return nil, err
	}

	done := rawresp.(*RpbListKeysResp).Done
	respresp := rawresp.(*RpbListKeysResp).Keys

	response = append(response, respresp...)

	for done == nil {
		moreresp, moreerr := c.Response(&RpbListKeysResp{})
		if moreerr != nil {
			if moreerr == ErrReadTimeout {
				continue
			} else {
				return nil, moreerr
			}
		}

		done = moreresp.(*RpbListKeysResp).Done
		response = append(response, moreresp.(*RpbListKeysResp).Keys...)
	}

	return response, nil
}

// Delete an Object from a bucket
func (c *Conn) DeleteObject(bucket string, key string) (response []byte, err error) {
	reqstruct := &RpbDelReq{
		Bucket: []byte(bucket),
		Key:    []byte(key),
	}

	err = c.Request(reqstruct, "RpbDelReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbGetResp{})
	if err != nil {
		return nil, err
	}

	response = uncoercedresponse.([]byte)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// Get server info
func (c *Conn) GetServerInfo() (response []byte, err error) {
	reqdata := []byte{}

	err = c.Request(reqdata, "RpbGetServerInfoReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbGetServerInfoResp{})

	if err != nil {
		return nil, err
	}

	response = uncoercedresponse.([]byte)

	return response, nil
}

// Ping the server
func (c *Conn) Ping() (response []byte, err error) {
	reqdata := []byte{}

	err = c.Request(reqdata, "RpbPingReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbPingResp{})

	response = uncoercedresponse.([]byte)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// Get bucket info
func (c *Conn) GetBucket(bucket string) (response []byte, err error) {
	reqstruct := &RpbGetBucketReq{
		Bucket: []byte(bucket),
	}

	err = c.Request(reqstruct, "RpbGetBucketReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbGetBucketResp{})
	if err != nil {
		return nil, err
	}

	response = uncoercedresponse.([]byte)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// Get client ID
func (c *Conn) GetClientId() (response []byte, err error) {
	reqdata := []byte{0, 0, 0, 1, 3}

	err = c.Request(reqdata, "RpbGetClientIdReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbGetClientIdResp{})

	response = uncoercedresponse.([]byte)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// Set client ID
func (c *Conn) SetClientId(clientId string) (response []byte, err error) {
	reqstruct := &RpbSetClientIdReq{
		ClientId: []byte(clientId),
	}

	err = c.Request(reqstruct, "RpbSetClientIdReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbSetClientIdResp{})
	if err != nil {
		return nil, err
	}

	response = uncoercedresponse.([]byte)
	if err != nil {
		return nil, err
	}

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
	rawresp, err = c.Response(&RpbMapRedResp{})
	if err != nil {
		return nil, err
	}

	done := rawresp.(*RpbMapRedResp).Done
	respresp := rawresp.(*RpbMapRedResp).Response

	response = append(response, respresp)

	for done == nil {
		moreresp, moreerr := c.Response(&RpbMapRedResp{})
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

	uncoercedresponse, err := c.Response(&RpbSetBucketResp{})
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

	uncoercedresponse, err := c.Response(&RpbListBucketsResp{})

	response = uncoercedresponse.([][]byte)
	if err != nil {
		return nil, err
	}

	return response, nil
}
