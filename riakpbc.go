package riakpbc

import (
	"errors"
	"reflect"
	"strconv"
)

type RpbEmptyResp struct{}

// FetchObject returns an object from a bucket.
//
// Pass RpbGetReq to SetOpts for optional parameters.
func (c *Conn) FetchObject(bucket, key string) (*RpbGetResp, error) {
	reqstruct := &RpbGetReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbGetReq)
	}
	reqstruct.Bucket = []byte(bucket)
	reqstruct.Key = []byte(key)

	if err := c.Request(reqstruct, "RpbGetReq"); err != nil {
		return &RpbGetResp{}, err
	}

	response, err := c.Response()
	if err != nil {
		return &RpbGetResp{}, err
	}

	return response.(*RpbGetResp), nil
}

// StoreObject puts an object with ky into bucket.
//
// Content can be passed as either a struct, RpbContent, raw string or binary.
//
// Use RpbContent if you need absolute control over what is going into Riak.
// Otherwise data conveniently gets wrapped for you.  Check Encoder.Marshall()
// for `riak` tags that can be set on a structure for automated indexes and links.
//
// Pass RpbPutReq to SetOpts for optional parameters.
func (c *Conn) StoreObject(bucket, key string, content interface{}) (*RpbPutResp, error) {
	reqstruct := &RpbPutReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbPutReq)
	}
	reqstruct.Bucket = []byte(bucket)
	reqstruct.Key = []byte(key)

	if _, ok := content.(*RpbContent); ok {
		reqstruct.Content = content.(*RpbContent)
	} else {
		// Determine the primitive type of content.
		t := reflect.TypeOf(content)

		if t.Kind() == reflect.Ptr { // struct or RpbContent
			switch t.Elem().Kind() {
			case reflect.Struct:
				e := NewEncoder()
				encctnt, err := e.Marshal(content)
				if err != nil {
					return nil, err
				}
				reqstruct.Content = encctnt
				break
			}
		} else { // string, int,  or []byte
			switch t.Kind() {
			case reflect.String:
				reqstruct.Content = &RpbContent{
					Value:       []byte(content.(string)),
					ContentType: []byte("plain/text"),
				}
				break
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				reqstruct.Content = &RpbContent{
					Value:       []byte(strconv.FormatInt(int64(content.(int)), 10)),
					ContentType: []byte("plain/text"),
				}
				break
			default:
				reqstruct.Content = &RpbContent{
					Value:       content.([]byte),
					ContentType: []byte("application/octet-stream"),
				}
				break
			}
		}
	}

	if reqstruct.Content == nil {
		return nil, errors.New("Invalid content type passed.  Must be struct, RpbContent, string, or []byte")
	}

	if err := c.Request(reqstruct, "RpbPutReq"); err != nil {
		return &RpbPutResp{}, err
	}

	response, err := c.Response()
	if err != nil {
		return &RpbPutResp{}, err
	}

	return response.(*RpbPutResp), nil
}

// DeleteObject removes object with key from bucket.
//
// Pass RpbDelReq to SetOpts for optional parameters.
func (c *Conn) DeleteObject(bucket, key string) ([]byte, error) {
	reqstruct := &RpbDelReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbDelReq)
	}
	reqstruct.Bucket = []byte(bucket)
	reqstruct.Key = []byte(key)

	if err := c.Request(reqstruct, "RpbDelReq"); err != nil {
		return nil, err
	}

	response, err := c.Response()
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}
