package riakpbc

type RpbEmptyResp struct{}

// Fetch an object from a bucket
func (c *Conn) FetchObject(bucket, key string, opts *RpbGetReq) (*RpbGetResp, error) {
	reqstruct := &RpbGetReq{}
	if opts != nil {
		reqstruct = opts
	}
	reqstruct.Bucket = []byte(bucket)
	reqstruct.Key = []byte(key)

	if err := c.Request(reqstruct, "RpbGetReq"); err != nil {
		return &RpbGetResp{}, err
	}

	response, err := c.Response(&RpbGetResp{})
	if err != nil {
		return &RpbGetResp{}, err
	}

	return response.(*RpbGetResp), nil
}

// Store an object in riak
func (c *Conn) StoreObject(bucket, key string, content []byte, contentType string, opts *RpbPutReq) (*RpbPutResp, error) {
	reqstruct := &RpbPutReq{}
	if opts != nil {
		reqstruct = opts
	}
	reqstruct.Bucket = []byte(bucket)
	reqstruct.Key = []byte(key)
	reqstruct.Content = &RpbContent{
		Value:       content,
		ContentType: []byte(contentType),
	}

	if err := c.Request(reqstruct, "RpbPutReq"); err != nil {
		return &RpbPutResp{}, err
	}

	response, err := c.Response(&RpbPutResp{})
	if err != nil {
		return &RpbPutResp{}, err
	}

	return response.(*RpbPutResp), nil
}

// Delete an Object from a bucket
func (c *Conn) DeleteObject(bucket, key string, opts *RpbDelReq) ([]byte, error) {
	reqstruct := &RpbDelReq{}
	if opts != nil {
		reqstruct = opts
	}
	reqstruct.Bucket = []byte(bucket)
	reqstruct.Key = []byte(key)

	if err := c.Request(reqstruct, "RpbDelReq"); err != nil {
		return nil, err
	}

	response, err := c.Response(&RpbGetResp{})
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}
