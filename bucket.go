package riakpbc

// List all buckets
func (c *Client) ListBuckets() (*RpbListBucketsResp, error) {
	reqdata := []byte{}

	response, err := c.ReqResp(reqdata, "RpbListBucketsReq", true)
	if err != nil {
		return nil, err
	}

	return response.(*RpbListBucketsResp), nil
}

// List all keys from bucket
func (c *Client) ListKeys(bucket string) ([][]byte, error) {
	reqstruct := &RpbListKeysReq{
		Bucket: []byte(bucket),
	}

	response, err := c.ReqResp(reqstruct, "RpbListKeysReq", false)
	if err != nil {
		return nil, err
	}

	keys := response.(*RpbListKeysResp).GetKeys()

	// i broke this - @mrb
	/*
		  done := response.(*RpbListKeysResp).GetDone()
			for done != true {
				response, err := node.Response()
				if err != nil {
					return nil, err
				}
				keys = append(keys, response.(*RpbListKeysResp).GetKeys()...)
				done = response.(*RpbListKeysResp).GetDone()
			}
	*/

	return keys, nil
}

// Get bucket info
func (c *Client) GetBucket(bucket string) (*RpbGetBucketResp, error) {
	reqstruct := &RpbGetBucketReq{
		Bucket: []byte(bucket),
	}

	response, err := c.ReqResp(reqstruct, "RpbGetBucketReq", false)
	if err != nil {
		return nil, err
	}

	return response.(*RpbGetBucketResp), nil
}

// Create bucket
func (c *Client) SetBucket(bucket string, nval *uint32, allowmult *bool) ([]byte, error) {
	reqstruct := &RpbSetBucketReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbSetBucketReq)
	}
	reqstruct.Bucket = []byte(bucket)
	if reqstruct.Props == nil {
		reqstruct.Props = &RpbBucketProps{}
		reqstruct.Props.NVal = nval
		reqstruct.Props.AllowMult = allowmult
	}

	response, err := c.ReqResp(reqstruct, "RpbSetBucketReq", false)
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}
