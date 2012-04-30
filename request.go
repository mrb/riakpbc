package riakpbc

import (
	"code.google.com/p/goprotobuf/proto"
)

var commandToNum = map[string]byte{
	"RpbErrorResp":         0,
	"RpbPingReq":           1,
	"RpbPingResp":          2,
	"RpbGetClientIdReq":    3,
	"RpbGetClientIdResp":   4,
	"RpbSetClientIdReq":    5,
	"RpbSetClientIdResp":   6,
	"RpbGetServerInfoReq":  7,
	"RpbGetServerInfoResp": 8,
	"RpbGetReq":            9,
	"RpbGetResp":           10,
	"RpbPutReq":            11,
	"RpbPutResp":           12,
	"RpbDelReq":            13,
	"RpbDelResp":           14,
	"RpbListBucketsReq":    15,
	"RpbListBucketsResp":   16,
	"RpbListKeysReq":       17,
	"RpbListKeysResp":      18,
	"RpbGetBucketReq":      19,
	"RpbGetBucketResp":     20,
	"RpbSetBucketReq":      21,
	"RpbSetBucketResp":     22,
	"RpbMapRedReq":         23,
	"RpbMapRedResp":        24,
}

func (c *Conn) Request(reqstruct interface{}, structname string) (err error) {
	marshaledRequest, err := marshalRequest(reqstruct)
	if err != nil {
		return err
	}

	formattedRequest, err := prependRequestHeader(structname, marshaledRequest)
	if err != nil {
		return err
	}

	err = c.Write(formattedRequest)
	if err != nil {
		return err
	}

	return nil
}

func prependRequestHeader(commandName string, marshaledReqData []byte) (formattedData []byte, e error) {
	msgbuf := []byte{}
	formattedData = []byte{}

	mn := []byte{0, 0, 0}
	comm := []byte{commandToNum[commandName]}

	msgbuf = append(msgbuf, comm...)
	msgbuf = append(msgbuf, marshaledReqData...)

	length := []byte{byte(len(msgbuf))}

	formattedData = append(formattedData, mn...)
	formattedData = append(formattedData, length...)
	formattedData = append(formattedData, msgbuf...)

	return formattedData, nil
}

func marshalRequest(reqstruct interface{}) (marshaledRequest []byte, err error) {
	marshaledRequest, err = proto.Marshal(reqstruct)

	return marshaledRequest, nil
}
