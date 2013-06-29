package riakpbc

// MapReduce executes a MapReduce job.
//
// Encodings:
//
//    - application/json - JSON-encoded map/reduce job
//    - application/x-erlang-binary - Erlang external term format
func (node *Node) MapReduce(request, contentType string) ([]byte, error) {
	reqstruct := &RpbMapRedReq{
		Request:     []byte(request),
		ContentType: []byte(contentType),
	}

	response, err := node.ReqMultiResp(reqstruct, "RpbMapRedReq")
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}

// Index requests a set of keys that match a secondary index query.
//
//     qtype - an IndexQueryType of either 0 (eq) or 1 (range)
func (node *Node) Index(bucket, index, key, start, end string) (*RpbIndexResp, error) {
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

	response, err := node.ReqResp(reqstruct, "RpbIndexReq", false)
	if err != nil {
		if err.Error() == "object not found" {
			return &RpbIndexResp{}, nil
		}
		return nil, err
	}

	return response.(*RpbIndexResp), nil
}

// Search scans bucket for query string q and searches index for the match.
//
// Pass RpbSearchQueryReq to SetOpts for optional parameters.
func (node *Node) Search(index, q string) (*RpbSearchQueryResp, error) {
	reqstruct := &RpbSearchQueryReq{}
	if opts := node.Opts(); opts != nil {
		reqstruct = opts.(*RpbSearchQueryReq)
	}
	reqstruct.Q = []byte(q)
	reqstruct.Index = []byte(index)

	response, err := node.ReqResp(reqstruct, "RpbSearchQueryReq", false)
	if err != nil {
		return nil, err
	}

	return response.(*RpbSearchQueryResp), nil
}
