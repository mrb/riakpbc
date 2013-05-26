package riakpbc

// MapReduce executes a MapReduce job.
//
// Encodings:
//
//    - application/json - JSON-encoded map/reduce job
//    - application/x-erlang-binary - Erlang external term format
func (c *Conn) MapReduce(request, contentType string) ([]byte, error) {
	reqstruct := &RpbMapRedReq{
		Request:     []byte(request),
		ContentType: []byte(contentType),
	}

	if err := c.Request(reqstruct, "RpbMapRedReq"); err != nil {
		return nil, err
	}

	response, err := c.Response(&RpbMapRedResp{})
	if err != nil {
		return nil, err
	}

	mapResponse := response.(*RpbMapRedResp).GetResponse()
	done := response.(*RpbMapRedResp).GetDone()
	for done != true {
		response, err := c.Response(&RpbMapRedResp{})
		if err != nil {
			return nil, err
		}
		mapResponse = append(mapResponse, response.(*RpbMapRedResp).GetResponse()...)
		done = response.(*RpbMapRedResp).GetDone()
	}

	return mapResponse, nil
}

// Index requests a set of keys that match a secondary index query.
//
//     qtype - an IndexQueryType of either 0 (eq) or 1 (range)
//
// Pass RpIndexReq to SetOpts for optional parameters.
func (c *Conn) Index(bucket, index string, qtype RpbIndexReq_IndexQueryType) (RpbIndexResp, error) {
	reqstruct := &RpbIndexReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbIndexReq)
	}
	reqstruct.Bucket = []byte(bucket)
	reqstruct.Index = []byte(index)
	reqstruct.Qtype = &qtype

	if err := c.Request(reqstruct, "RpbIndexReq"); err != nil {
		return RpbIndexResp{}, err
	}

	response, err := c.Response(&RpbIndexResp{})
	if err != nil {
		return RpbIndexResp{}, err
	}

	return response.(RpbIndexResp), nil
}

// Search scans bucket for query string q and searches index for the match.
//
// Pass RpbSearchQueryReq to SetOpts for optional parameters.
func (c *Conn) Search(q, index string) (RpbSearchQueryResp, error) {
	reqstruct := &RpbSearchQueryReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbSearchQueryReq)
	}
	reqstruct.Q = []byte(q)
	reqstruct.Index = []byte(index)

	if err := c.Request(reqstruct, "RpbSearchQueryReq"); err != nil {
		return RpbSearchQueryResp{}, err
	}

	response, err := c.Response(&RpbSearchQueryResp{})
	if err != nil || response == nil {
		return RpbSearchQueryResp{}, err
	}

	return response.(RpbSearchQueryResp), nil
}
