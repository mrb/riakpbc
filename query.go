package riakpbc

// NewMapReduceRequest prepares a new MapReduce request.
func (c *Client) NewMapReduceRequest(request, contentType string) *RpbMapRedReq {
	return &RpbMapRedReq{
		Request:     []byte(request),
		ContentType: []byte(contentType),
	}
}

func (c *Client) mapReduce(opts *RpbMapRedReq, request, contentType string) ([]byte, error) {
	if opts == nil {
		opts = c.NewMapReduceRequest(request, contentType)
	}

	response, err := c.ReqMultiResp(opts, "RpbMapRedReq")
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}

// MapReduce executes a MapReduce job.
//
// Encodings:
//
//    - application/json - JSON-encoded map/reduce job
//    - application/x-erlang-binary - Erlang external term format
func (c *Client) MapReduce(request, contentType string) ([]byte, error) {
	return c.mapReduce(nil, request, contentType)
}

// NewIndexRequest prepares a new Index request.
func (c *Client) NewIndexRequest(bucket, index, key, start, end string) *RpbIndexReq {
	opts := &RpbIndexReq{
		Bucket: []byte(bucket),
		Index:  []byte(index),
	}

	var qType RpbIndexReq_IndexQueryType
	if key != "" {
		qType = 0
		opts.Qtype = &qType
		opts.Key = []byte(key)
	} else {
		qType = 1
		opts.Qtype = &qType
		opts.RangeMin = []byte(start)
		opts.RangeMax = []byte(end)
	}

	return opts
}

func (c *Client) index(opts *RpbIndexReq, bucket, index, key, start, end string) (*RpbIndexResp, error) {
	if opts == nil {
		opts = c.NewIndexRequest(bucket, index, key, start, end)
	}

	response, err := c.ReqResp(opts, "RpbIndexReq", false)
	if err != nil {
		if err.Error() == "object not found" {
			return &RpbIndexResp{}, nil
		}
		return nil, err
	}

	return response.(*RpbIndexResp), nil
}

// Index requests a set of keys that match a secondary index query.
//
//     qtype - an IndexQueryType of either 0 (eq) or 1 (range)
func (c *Client) Index(bucket, index, key, start, end string) (*RpbIndexResp, error) {
	return c.index(nil, bucket, index, key, start, end)
}

// NewSearchRequest prepares a new Search request.
func (c *Client) NewSearchRequest(index, q string) *RpbSearchQueryReq {
	return &RpbSearchQueryReq{
		Q:     []byte(q),
		Index: []byte(index),
	}
}

func (c *Client) search(opts *RpbSearchQueryReq, index, q string) (*RpbSearchQueryResp, error) {
	if opts == nil {
		opts = c.NewSearchRequest(index, q)
	}

	response, err := c.ReqResp(opts, "RpbSearchQueryReq", false)
	if err != nil {
		return nil, err
	}

	return response.(*RpbSearchQueryResp), nil
}

// Search scans bucket for query string q and searches index for the match.
func (c *Client) Search(index, q string) (*RpbSearchQueryResp, error) {
	return c.search(nil, index, q)
}
