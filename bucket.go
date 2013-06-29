package riakpbc

// List all buckets
func (node *Node) ListBuckets() (*RpbListBucketsResp, error) {
	reqdata := []byte{}

	response, err := node.ReqResp(reqdata, "RpbListBucketsReq", true)
	if err != nil {
		return nil, err
	}

	return response.(*RpbListBucketsResp), nil
}

// List all keys from bucket
func (node *Node) ListKeys(bucket string) ([][]byte, error) {
	reqstruct := &RpbListKeysReq{
		Bucket: []byte(bucket),
	}

	response, err := node.ReqMultiResp(reqstruct, "RpbListKeysReq")
	if err != nil {
		return nil, err
	}

	keys := response.([][]byte)

	return keys, nil
}

// Get bucket info
func (node *Node) GetBucket(bucket string) (*RpbGetBucketResp, error) {
	reqstruct := &RpbGetBucketReq{
		Bucket: []byte(bucket),
	}

	response, err := node.ReqResp(reqstruct, "RpbGetBucketReq", false)
	if err != nil {
		return nil, err
	}

	return response.(*RpbGetBucketResp), nil
}

// Create bucket
func (node *Node) SetBucket(bucket string, nval *uint32, allowmult *bool) ([]byte, error) {
	reqstruct := &RpbSetBucketReq{}
	if opts := node.Opts(); opts != nil {
		reqstruct = opts.(*RpbSetBucketReq)
	}
	reqstruct.Bucket = []byte(bucket)
	if reqstruct.Props == nil {
		reqstruct.Props = &RpbBucketProps{}
		reqstruct.Props.NVal = nval
		reqstruct.Props.AllowMult = allowmult
	}

	response, err := node.ReqResp(reqstruct, "RpbSetBucketReq", false)
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}
