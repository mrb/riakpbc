package riakpbc

// MapReduce executes a MapReduce job.
//
// Encodings:
//
//    - application/json - JSON-encoded map/reduce job
//    - application/x-erlang-binary - Erlang external term format
func (c *Conn) MapReduce(request, contentType string) (response [][]byte, err error) {
	reqstruct := &RpbMapRedReq{
		Request:     []byte(request),
		ContentType: []byte(contentType),
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

// Index requests a set of keys that match a secondary index query.
//
//     qtype - an IndexQueryType of either 0 (eq) or 1 (range)
func (c *Conn) Index(bucket, index string, qtype RpbIndexReq_IndexQueryType, opts *RpbIndexReq) (response RpbIndexResp, err error) {
	reqstruct := &RpbIndexReq{}
	if opts != nil {
		reqstruct = opts
	}
	reqstruct.Bucket = []byte(bucket)
	reqstruct.Index = []byte(index)
	reqstruct.Qtype = &qtype

	err = c.Request(reqstruct, "RpbIndexReq")
	if err != nil {
		return RpbIndexResp{}, err
	}

	rawresp, err := c.Response(&RpbIndexResp{})
	if err != nil {
		return RpbIndexResp{}, err
	}

	if rawresp != nil {
		return rawresp.(RpbIndexResp), nil
	}

	return RpbIndexResp{}, nil
}

// Search scans bucket for query string q and searches index for the match.
//
// RpbSearchQueryReq can be passed in to further enhance the query, otherwise pass nil.
func (c *Conn) Search(q, index string, opts *RpbSearchQueryReq) (response RpbSearchQueryResp, err error) {
	reqstruct := &RpbSearchQueryReq{}
	if opts != nil {
		reqstruct = opts
	}
	reqstruct.Q = []byte(q)
	reqstruct.Index = []byte(index)

	err = c.Request(reqstruct, "RpbSearchQueryReq")
	if err != nil {
		return RpbSearchQueryResp{}, err
	}

	rawresp, err := c.Response(&RpbSearchQueryResp{})
	if err != nil {
		return RpbSearchQueryResp{}, err
	}

	if rawresp != nil {
		return rawresp.(RpbSearchQueryResp), nil
	}

	return RpbSearchQueryResp{}, nil
}
