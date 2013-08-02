package riakpbc

// ListBuckets lists all buckets.
func (c *Client) ListBuckets() (*RpbListBucketsResp, error) {
	opts := []byte{}

	response, err := c.ReqResp(opts, "RpbListBucketsReq", true)
	if err != nil {
		return nil, err
	}

	return response.(*RpbListBucketsResp), nil
}

// ListKeysRequest prepares a ListKeys request.
func (c *Client) NewListKeysRequest(bucket string) *RpbListKeysReq {
	return &RpbListKeysReq{
		Bucket: []byte(bucket),
	}
}

func (c *Client) listKeys(opts *RpbListKeysReq, bucket string) ([][]byte, error) {
	if opts == nil {
		opts = c.NewListKeysRequest(bucket)
	}

	response, err := c.ReqMultiResp(opts, "RpbListKeysReq")
	if err != nil {
		return nil, err
	}

	keys := response.([][]byte)

	return keys, nil
}

// ListKeys lists all keys from bucket.
func (c *Client) ListKeys(bucket string) ([][]byte, error) {
	return c.listKeys(nil, bucket)
}

// NewGetBucketRequest prepares a GetBucket request.
func (c *Client) NewGetBucketRequest(bucket string) *RpbGetBucketReq {
	return &RpbGetBucketReq{
		Bucket: []byte(bucket),
	}
}

func (c *Client) getBucket(opts *RpbGetBucketReq, bucket string) (*RpbGetBucketResp, error) {
	if opts == nil {
		opts = c.NewGetBucketRequest(bucket)
	}

	response, err := c.ReqResp(opts, "RpbGetBucketReq", false)
	if err != nil {
		return nil, err
	}

	return response.(*RpbGetBucketResp), nil
}

// GetBucket gets the bucket info.
func (c *Client) GetBucket(bucket string) (*RpbGetBucketResp, error) {
	return c.getBucket(nil, bucket)
}

// NewSetBucketRequest prepares a SetBucket request.
func (c *Client) NewSetBucketRequest(bucket string, nval *uint32, allowmult *bool) *RpbSetBucketReq {
	return &RpbSetBucketReq{
		Bucket: []byte(bucket),
		Props: &RpbBucketProps{
			NVal:      nval,
			AllowMult: allowmult,
		},
	}
}

func (c *Client) setBucket(opts *RpbSetBucketReq, bucket string, nval *uint32, allowmult *bool) ([]byte, error) {
	if opts == nil {
		opts = c.NewSetBucketRequest(bucket, nval, allowmult)
	}

	response, err := c.ReqResp(opts, "RpbSetBucketReq", false)
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}

// SetBucket sets the bucket info.
func (c *Client) SetBucket(bucket string, nval *uint32, allowmult *bool) ([]byte, error) {
	return c.setBucket(nil, bucket, nval, allowmult)
}
