package riakpbc

type RpbEmptyResp struct{}

// Store an object in riak
func (c *Conn) StoreObject(bucket, key string, content []byte, contentType string) (response []byte, err error) {
	reqstruct := &RpbPutReq{
		Bucket: []byte(bucket),
		Key:    []byte(key),
		Content: &RpbContent{
			Value:       content,
			ContentType: []byte(contentType),
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

	return uncoercedresponse.([]byte), nil
}

// Fetch an object from a bucket
func (c *Conn) FetchObject(bucket, key string) (response []byte, err error) {
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

	return uncoercedresponse.([]byte), nil
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
		return nil, ErrNotDone
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
func (c *Conn) DeleteObject(bucket, key string) (response []byte, err error) {
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

	return uncoercedresponse.([]byte), nil
}

// Get server info
func (c *Conn) GetServerInfo() (response []byte, err error) {
	reqdata := []byte{}

	err = c.RawRequest(reqdata, "RpbGetServerInfoReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbGetServerInfoResp{})
	if err != nil {
		return nil, err
	}

	return uncoercedresponse.([]byte), nil
}

// Ping the server
func (c *Conn) Ping() (response []byte, err error) {
	reqdata := []byte{}

	err = c.RawRequest(reqdata, "RpbPingReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbEmptyResp{})
	if err != nil {
		return nil, err
	}

	return uncoercedresponse.([]byte), nil
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

	return uncoercedresponse.([]byte), nil
}

// Get client ID
func (c *Conn) GetClientId() (response []byte, err error) {
	reqdata := []byte{}

	err = c.RawRequest(reqdata, "RpbGetClientIdReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbGetClientIdResp{})
	if err != nil {
		return nil, err
	}

	return uncoercedresponse.([]byte), nil
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

	uncoercedresponse, err := c.Response(&RpbSetClientIdReq{})
	if err != nil {
		return nil, err
	}

	return uncoercedresponse.([]byte), nil
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

	uncoercedresponse, err := c.Response(&RpbEmptyResp{})
	if err != nil {
		return nil, err
	}

	return uncoercedresponse.([]byte), nil
}

// List all buckets
func (c *Conn) ListBuckets() (response [][]byte, err error) {
	reqdata := []byte{}

	err = c.RawRequest(reqdata, "RpbListBucketsReq")
	if err != nil {
		return nil, err
	}

	uncoercedresponse, err := c.Response(&RpbListBucketsResp{})
	if err != nil {
		return nil, err
	}

	return uncoercedresponse.([][]byte), nil
}
