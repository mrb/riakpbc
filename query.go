package riakpbc

// MapReduce executes a MapReduce job.
//
// Encodings:
//
//    - application/json - JSON-encoded map/reduce job
//    - application/x-erlang-binary - Erlang external term format
func (c *Client) MapReduce(request, contentType string) ([]byte, error) {
	reqstruct := &RpbMapRedReq{
		Request:     []byte(request),
		ContentType: []byte(contentType),
	}

	response, err := c.ReqResp(reqstruct, "RpbMapRedReq", false)
	if err != nil {
		return nil, err
	}

	mapResponse := response.(*RpbMapRedResp).GetResponse()
	// I broke this - @mrb
	/*
		done := response.(*RpbMapRedResp).GetDone()
		for done != true {
			response, err := node.Response()
			if err != nil {
				return nil, err
			}
			mapResponse = append(mapResponse, response.(*RpbMapRedResp).GetResponse()...)
			done = response.(*RpbMapRedResp).GetDone()
		}
	*/

	return mapResponse, nil
}

// Index requests a set of keys that match a secondary index query.
//
//     qtype - an IndexQueryType of either 0 (eq) or 1 (range)
func (c *Client) Index(bucket, index, key, start, end string) (*RpbIndexResp, error) {
	reqstruct := &RpbIndexReq{}
	reqstruct.Bucket = []byte(bucket)
	reqstruct.Index = []byte(index)

	var qType RpbIndexReq_IndexQueryType
	if key != "" {
		qType = 0
		reqstruct.Qtype = &qType
		reqstruct.Key = []byte(key)
	} else {
		qType = 1
		reqstruct.Qtype = &qType
		reqstruct.RangeMin = []byte(start)
		reqstruct.RangeMax = []byte(end)
	}

	response, err := c.ReqResp(reqstruct, "RpbIndexReq", false)
	if err != nil {
		return nil, err
	}

	return response.(*RpbIndexResp), nil
}

// Search scans bucket for query string q and searches index for the match.
//
// Pass RpbSearchQueryReq to SetOpts for optional parameters.
func (c *Client) Search(index, q string) (*RpbSearchQueryResp, error) {
	reqstruct := &RpbSearchQueryReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbSearchQueryReq)
	}
	reqstruct.Q = []byte(q)
	reqstruct.Index = []byte(index)

	response, err := c.ReqResp(reqstruct, "RpbSearchQueryReq", false)
	if err != nil {
		return nil, err
	}

	return response.(*RpbSearchQueryResp), nil
}
