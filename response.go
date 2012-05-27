package riakpbc

import (
	"code.google.com/p/goprotobuf/proto"
	"log"
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
}

var (
	maxReadRetries = 3
)

func (c *Conn) Response(respstruct interface{}, structname string) (response interface{}, err error) {
	currentRetries := 0
	var rawresp []byte
	rawresp, err = c.Read()

	if err != nil {
		if err == ErrReadTimeout && currentRetries < maxReadRetries {
			for currentRetries < maxReadRetries {
				log.Print(currentRetries, maxReadRetries, rawresp)
				rawresp, err = c.Read()
				if err != nil {
					currentRetries = currentRetries + 1
				} else {
					currentRetries = maxReadRetries + 1
				}
			}
		}
		err = ErrReadTimeout
		return nil, err
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
	if len(respraw) < 5 {
		err = ErrCorruptHeader
		return err
	}

	resplength := int(respraw[3])

	if resplength < 0 {
		err = ErrLengthZero
		return err
	}

	if resplength == 0 {
		err = ErrLengthZero
		return err
	}

	resptype := respraw[4]

	if resptype < 0 || resptype > 24 {
		err = ErrNoSuchCommand
		return err
	}

	if resptype == 0 {
		err = ErrRiakError
		return err
	}

	return nil
}

func unmarshalResponse(respraw []byte) (respbuf interface{}, err error) {
	resptype := respraw[4]
	resplength := int(respraw[3])
	structname := numToCommand[int(resptype)]
	respbuf = respraw[5:]

	if resplength+3 > 5 {
		respbuf = respraw[5 : resplength+4]
	}

	if structname == "RpbGetResp" {
		respstruct := &RpbGetResp{}
		if resplength == 1 {
			err = ErrObjectNotFound
			return nil, err
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
		respbuf = respstruct.Keys
	}

	if structname == "RpbPutResp" {
		respstruct := &RpbPutResp{}
		if resplength == 1 {
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

	return respbuf, nil
}
