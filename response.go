package riakpbc

import (
	"code.google.com/p/goprotobuf/proto"
)

var numToCommand = map[int]string{
	0:  "RpbErrorResp",
	1:  "RpbPingReq",
	2:  "RpbPingResp",
	3:  "RpbGetClientIdReq",
	4:  "RpbGetClientIdResp",
	5:  "RpbSetClientIdReq",
	6:  "RpbSetClientIdResp",
	7:  "RpbGetServerInfoReq",
	8:  "RpbGetServerInfoResp",
	9:  "RpbGetReq",
	10: "RpbGetResp",
	11: "RpbPutReq",
	12: "RpbPutResp",
	13: "RpbDelReq",
	14: "RpbDelResp",
	15: "RpbListBucketsReq",
	16: "RpbListBucketsResp",
	17: "RpbListKeysReq",
	18: "RpbListKeysResp",
	19: "RpbGetBucketReq",
	20: "RpbGetBucketResp",
	21: "RpbSetBucketReq",
	22: "RpbSetBucketResp",
	23: "RpbMapRedReq",
	24: "RpbMapRedResp",
	25: "RpbIndexReq",
	26: "RpbIndexResp",
	27: "RpbSearchQueryReq",
	28: "RbpSearchQueryResp",
}

var (
	maxReadRetries = 3
)

func (c *Conn) Response(respstruct interface{}) (response interface{}, err error) {
	currentRetries := 0
	var rawresp []byte
	rawresp, err = c.Read()
	if err != nil {
		if err == ErrReadTimeout && currentRetries < maxReadRetries {
			for currentRetries < maxReadRetries {
				rawresp, err = c.Read()
				if err != nil {
					currentRetries = currentRetries + 1
				} else if currentRetries > maxWriteRetries {
					return nil, ErrReadTimeout
				} else {
					break // success
				}
			}
		}
	}

	err = validateResponseHeader(rawresp)
	if err != nil {
		return nil, err
	}

	response, err = unmarshalResponse(rawresp)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func validateResponseHeader(respraw []byte) (err error) {
	if len(respraw) < 1 {
		return ErrCorruptHeader
	}

	resptype := respraw[0]

	if resptype < 0 || resptype > 28 {
		return ErrNoSuchCommand
	}

	if resptype == 0 {
		return ErrRiakError
	}

	return nil
}

func unmarshalResponse(respraw []byte) (respbuf interface{}, err error) {
	reslength := len(respraw)
	resptype := respraw[0]
	structname := numToCommand[int(resptype)]

	if reslength > 1 {
		respbuf = respraw[1:]
	}

	if structname == "RpbGetResp" {
		respstruct := &RpbGetResp{}
		if reslength == 1 {
			return nil, ErrObjectNotFound
		}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		respbuf = respstruct.Content[0].Value
	}

	if structname == "RpbGetServerInfoResp" {
		respstruct := &RpbGetServerInfoResp{}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		respbuf = respstruct.Node
	}

	if structname == "RpbListBucketsResp" {
		respstruct := &RpbListBucketsResp{}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		respbuf = respstruct.Buckets
	}

	if structname == "RpbListKeysResp" {
		respstruct := &RpbListKeysResp{}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		respbuf = respstruct
	}

	if structname == "RpbGetClientIdResp" {
		respstruct := &RpbGetClientIdResp{}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		respbuf = respstruct.ClientId
	}

	if structname == "RpbSetClientIdResp" {
		if reslength == 1 {
			return []byte("Success"), nil
		}
		return nil, ErrObjectNotFound
	}

	if structname == "RpbPutResp" {
		respstruct := &RpbPutResp{}
		if reslength == 1 {
			return []byte("Success"), nil
		}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		respbuf = respstruct.Content
	}

	if structname == "RpbMapRedResp" {
		respstruct := &RpbMapRedResp{}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		respbuf = respstruct
	}

	if structname == "RpbSetBucketResp" {
		respbuf = []byte("Success")
		return respbuf, nil
	}

	if structname == "RpbGetBucketResp" {
		if reslength == 1 {
			return nil, ErrObjectNotFound
		}

		respstruct := &RpbGetBucketResp{}

		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		if err != nil {
			return nil, err
		}

		respbuf = []byte(respstruct.Props.String())

		return respbuf, nil
	}

	if structname == "RpbDelResp" {
		if reslength == 1 {
			respbuf = []byte("Success")
		}
		return respbuf, nil
	}

	if structname == "RpbPingResp" {
		if reslength == 1 {
			respbuf = []byte("Pong")
		}
		return respbuf, nil
	}

	return respbuf, nil
}
