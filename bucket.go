package riakpbc

// List all buckets
func (c *Conn) ListBuckets() (*RpbListBucketsResp, error) {
	reqdata := []byte{}

	if err := c.RawRequest(reqdata, "RpbListBucketsReq"); err != nil {
		return &RpbListBucketsResp{}, err
	}

	response, err := c.Response(&RpbListBucketsResp{})
	if err != nil {
		return &RpbListBucketsResp{}, err
	}

	return response.(*RpbListBucketsResp), nil
}

// List all keys from bucket
func (c *Conn) ListKeys(bucket string) (*RpbListKeysResp, error) {
	reqstruct := &RpbListKeysReq{
		Bucket: []byte(bucket),
	}

	if err := c.Request(reqstruct, "RpbListKeysReq"); err != nil {
		return &RpbListKeysResp{}, err
	}

	response, err := c.Response(&RpbListKeysResp{})
	if err != nil {
		return &RpbListKeysResp{}, err
	}

	return response.(*RpbListKeysResp), nil
}

// Get bucket info
func (c *Conn) GetBucket(bucket string) (*RpbGetBucketResp, error) {
	reqstruct := &RpbGetBucketReq{
		Bucket: []byte(bucket),
	}

	if err := c.Request(reqstruct, "RpbGetBucketReq"); err != nil {
		return &RpbGetBucketResp{}, err
	}

	response, err := c.Response(&RpbGetBucketResp{})
	if err != nil {
		return &RpbGetBucketResp{}, err
	}

	return response.(*RpbGetBucketResp), nil
}

// Create bucket
func (c *Conn) SetBucket(bucket string, nval *uint32, allowmult *bool) ([]byte, error) {
	propstruct := &RpbBucketProps{
		NVal:      nval,
		AllowMult: allowmult,
	}

	reqstruct := &RpbSetBucketReq{
		Bucket: []byte(bucket),
		Props:  propstruct,
	}

	if err := c.Request(reqstruct, "RpbSetBucketReq"); err != nil {
		return nil, err
	}

	response, err := c.Response(&RpbEmptyResp{})
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}
