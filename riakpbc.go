package riakpbc

import (
	"errors"
	"reflect"
	"strconv"
)

type RpbEmptyResp struct{}

// FetchObject returns an object from a bucket and returns a RpbGetResp response.
//
// Pass RpbGetReq to SetOpts for optional parameters.
func (c *Conn) FetchObject(bucket, key string) (*RpbGetResp, error) {
	reqstruct := &RpbGetReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbGetReq)
	}
	reqstruct.Bucket = []byte(bucket)
	reqstruct.Key = []byte(key)

	node := c.SelectNode()
	if err := node.Request(reqstruct, "RpbGetReq"); err != nil {
		return &RpbGetResp{}, err
	}

	response, err := node.Response()
	if err != nil {
		return &RpbGetResp{}, err
	}

	return response.(*RpbGetResp), nil
}

// StoreObject puts an object with key into bucket and returns a RpbGetResp response.
//
// The `in` content can be passed as either a RpbContent, string, int, or []byte.
//
// Use RpbContent if you need absolute control over what is going into Riak.
//
// Pass RpbPutReq to SetOpts for optional parameters.
func (c *Conn) StoreObject(bucket, key string, in interface{}) (*RpbPutResp, error) {
	reqstruct := &RpbPutReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbPutReq)
	}
	reqstruct.Bucket = []byte(bucket)
	reqstruct.Key = []byte(key)

	if _, ok := in.(*RpbContent); ok {
		reqstruct.Content = in.(*RpbContent)
	} else {
		// Determine the primitive type of content.
		t := reflect.TypeOf(in)

		switch t.Kind() {
		case reflect.String:
			reqstruct.Content = &RpbContent{
				Value:       []byte(in.(string)),
				ContentType: []byte("plain/text"),
			}
			break
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			reqstruct.Content = &RpbContent{
				Value:       []byte(strconv.FormatInt(int64(in.(int)), 10)),
				ContentType: []byte("plain/text"),
			}
			break
		default:
			reqstruct.Content = &RpbContent{
				Value:       in.([]byte),
				ContentType: []byte("application/octet-stream"),
			}
			break
		}
	}

	if reqstruct.Content == nil {
		return nil, errors.New("Invalid content type passed.  Must be RpbContent, string, int, or []byte")
	}

	node := c.SelectNode()

	if err := node.Request(reqstruct, "RpbPutReq"); err != nil {
		return &RpbPutResp{}, err
	}

	response, err := node.Response()
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

	node := c.SelectNode()

	if err := node.Request(reqstruct, "RpbDelReq"); err != nil {
		return nil, err
	}

	response, err := node.Response()
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}

// FetchStruct returns an object from a bucket and unmarshals it into the passed struct.
//
// Pass RpbGetReq to SetOpts for optional parameters.
func (c *Conn) FetchStruct(bucket, key string, out interface{}) error {
	if c.Coder == nil {
		panic("Cannot fetch to a struct unless a coder has been set")
	}

	reqstruct := &RpbGetReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbGetReq)
	}
	reqstruct.Bucket = []byte(bucket)
	reqstruct.Key = []byte(key)

	node := c.SelectNode()

	if err := node.Request(reqstruct, "RpbGetReq"); err != nil {
		return err
	}

	response, err := node.Response()
	if err != nil {
		return err
	}

	t := reflect.TypeOf(out)
	if t.Kind() == reflect.Ptr { // struct
		switch t.Elem().Kind() {
		// Structs get passed through a marshaller
		case reflect.Struct:
			// TODO: This only returns the first result.
			//  I believe the other possible results are related to vlocks, and will eventually need to be addressed.
			err := c.Coder.Unmarshal(response.(*RpbGetResp).GetContent()[0].GetValue(), out)
			if err != nil {
				return err
			}
		default:
			panic("Invalid out struct type passed to FetchStruct")
		}
	}

	return nil
}

// StoreStruct marshals the data from a struct, and adds it with key into bucket.
//
// Check Coder.Marshall() for `riak` tags that can be set on a structure for automated indexes and links.
//
// Pass RpbPutReq to SetOpts for optional parameters.
func (c *Conn) StoreStruct(bucket, key string, in interface{}) (*RpbPutResp, error) {
	if c.Coder == nil {
		panic("Cannot store a struct unless a coder has been set")
	}

	reqstruct := &RpbPutReq{}
	if opts := c.Opts(); opts != nil {
		reqstruct = opts.(*RpbPutReq)
	}
	reqstruct.Bucket = []byte(bucket)
	reqstruct.Key = []byte(key)

	if _, ok := in.(*RpbContent); ok {
		reqstruct.Content = in.(*RpbContent)
	} else {
		// Determine the primitive type of content.
		t := reflect.TypeOf(in)

		if t.Kind() == reflect.Ptr { // struct or RpbContent
			switch t.Elem().Kind() {
			case reflect.Struct:
				// Structs get passed through a marshaller
				encctnt, err := c.Coder.Marshal(in)
				if err != nil {
					return nil, err
				}
				reqstruct.Content = encctnt
				break
			default:
				panic("Invalid in struct type passed to StoreStruct")
			}
		}
	}

	if reqstruct.Content == nil {
		return nil, errors.New("Invalid content type passed.  Must be struct, RpbContent, string, int, or []byte")
	}

	node := c.SelectNode()

	if err := node.Request(reqstruct, "RpbPutReq"); err != nil {
		return &RpbPutResp{}, err
	}

	response, err := node.Response()
	if err != nil {
		return &RpbPutResp{}, err
	}

	return response.(*RpbPutResp), nil
}
