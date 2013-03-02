package riakpbc

import (
	"code.google.com/p/goprotobuf/proto"
        "bytes"
        "encoding/binary"
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

var (
	maxWriteRetries = 3
)

func (c *Conn) Request(reqstruct interface{}, structname string) (err error) {
	marshaledRequest, err := marshalRequest(reqstruct)

	if err != nil {
		return err
	}

	return c.RawRequest(marshaledRequest, structname)
}

func (c *Conn) RawRequest(marshaledRequest []byte, structname string) (err error) {
	formattedRequest, err := prependRequestHeader(structname, marshaledRequest)
	if err != nil {
		return err
	}

	currentRetries := 0
	err = c.Write(formattedRequest)
	if err != nil {
		return err
	}

	if err != nil {
		if err == ErrReadTimeout && currentRetries < maxReadRetries {
			for currentRetries < maxReadRetries {
				err = c.Write(formattedRequest)
				if err != nil {
					currentRetries = currentRetries + 1
				} else {
					currentRetries = maxReadRetries + 1
				}
			}
		}
		err = ErrWriteTimeout
		return err
	}

	return nil

}

func prependRequestHeader(commandName string, marshaledReqData []byte) (formattedData []byte, e error) {
	msgbuf := []byte{}
	formattedData = []byte{}

	comm := []byte{commandToNum[commandName]}

	msgbuf = append(msgbuf, comm...)
	msgbuf = append(msgbuf, marshaledReqData...)

        lenbuf := new(bytes.Buffer)
        binary.Write(lenbuf, binary.BigEndian, int32(len(msgbuf)))

        length := lenbuf.Bytes()

	formattedData = append(formattedData, length...)
	formattedData = append(formattedData, msgbuf...)

	return formattedData, nil
}

func marshalRequest(reqstruct interface{}) (marshaledRequest []byte, err error) {
	marshaledRequest, err = proto.Marshal(reqstruct.(proto.Message))
	return marshaledRequest, nil
}
