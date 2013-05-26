package riakpbc

type RpbEmptyResp struct{}

// FetchObject returns an object from a bucket.
//
// Pass RpbGetReq to SetOpts for optional parameters.
func (c *Conn) FetchObject(bucket, key string) (*RpbGetResp, error) {
	reqstruct := &RpbGetReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbGetReq)
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

// StoreObject puts an object with ky into bucket.
//
// Pass RpbPutReq to SetOpts for optional parameters.
func (c *Conn) StoreObject(bucket, key string, content []byte, contentType string) (*RpbPutResp, error) {
	reqstruct := &RpbPutReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbPutReq)
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

// DeleteObject removes object with key from bucket.
//
// Pass RpbDelReq to SetOpts for optional parameters.
func (c *Conn) DeleteObject(bucket, key string) ([]byte, error) {
	reqstruct := &RpbDelReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbDelReq)
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
