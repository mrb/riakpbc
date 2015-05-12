package riakpbc

import (
	"github.com/golang/protobuf/proto"
	"errors"
	"strconv"
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
	28: "RpbSearchQueryResp",
}

var (
	maxReadRetries = 3
)

func validateResponseHeader(respraw []byte) (err error) {
	if len(respraw) < 1 {
		return ErrCorruptHeader
	}

	resptype := respraw[0]

	if resptype < 0 || resptype > 28 {
		return ErrNoSuchCommand
	}

	return nil
}

func unmarshalResponse(respraw []byte) (respbuf interface{}, err error) {
	resplength := len(respraw)
	resptype := respraw[0]
	structname := numToCommand[int(resptype)]

	if resplength > 1 {
		respbuf = respraw[1:]
	}

	switch structname {

	case "RpbErrorResp":
		respstruct := &RpbErrorResp{}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(strconv.Itoa(int(respstruct.GetErrcode())) + ": " + string(string(respstruct.GetErrmsg())))

	case "RpbPingResp":
		return []byte("Pong"), nil

	case "RpbGetClientIdResp":
		respstruct := &RpbGetClientIdResp{}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		if err != nil {
			return nil, err
		}
		return respstruct, nil

	case "RpbSetClientIdResp":
		return []byte("Success"), nil

	case "RpbGetServerInfoResp":
		respstruct := &RpbGetServerInfoResp{}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		if err != nil {
			return nil, err
		}
		return respstruct, nil

	case "RpbGetResp":
		respstruct := &RpbGetResp{}
		if resplength == 1 {
			return nil, ErrObjectNotFound
		}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		if err != nil {
			return nil, err
		}
		return respstruct, nil

	case "RpbPutResp":
		respstruct := &RpbPutResp{}
		if resplength == 1 {
			return respstruct, nil
		}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		if err != nil {
			return nil, err
		}
		return respstruct, nil

	case "RpbDelResp":
		return []byte("Success"), nil

	case "RpbListBucketsResp":
		respstruct := &RpbListBucketsResp{}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		if err != nil {
			return nil, err
		}
		return respstruct, nil

	case "RpbListKeysResp":
		respstruct := &RpbListKeysResp{}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		if err != nil {
			return nil, err
		}
		return respstruct, nil

	case "RpbGetBucketResp":
		if resplength == 1 {
			return nil, ErrObjectNotFound
		}

		respstruct := &RpbGetBucketResp{}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		if err != nil {
			return nil, err
		}
		return respstruct, nil

	case "RpbSetBucketResp":
		return []byte("Success"), nil

	case "RpbMapRedResp":
		respstruct := &RpbMapRedResp{}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		if err != nil {
			return nil, err
		}
		return respstruct, nil

	case "RpbIndexResp":
		respstruct := &RpbIndexResp{}
		if resplength == 1 {
			return nil, ErrObjectNotFound
		}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		if err != nil {
			return nil, err
		}
		return respstruct, nil

	case "RpbSearchQueryResp":
		respstruct := &RpbSearchQueryResp{}
		err = proto.Unmarshal(respbuf.([]byte), respstruct)
		if err != nil {
			return nil, err
		}
		return respstruct, nil
	}

	return nil, nil
}
