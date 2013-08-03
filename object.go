package riakpbc

import (
	"errors"
	"reflect"
	"strconv"
)

type RpbEmptyResp struct{}

// NewFetchObjectRequest prepares a FetchObject request.
func (c *Client) NewFetchObjectRequest(bucket, key string) *RpbGetReq {
	return &RpbGetReq{
		Bucket: []byte(bucket),
		Key:    []byte(key),
	}
}

func (c *Client) fetchObject(opts *RpbGetReq, bucket, key string) (*RpbGetResp, error) {
	if opts == nil {
		opts = c.NewFetchObjectRequest(bucket, key)
	}

	response, err := c.ReqResp(opts, "RpbGetReq", false)
	if err != nil {
		return nil, err
	}

	return response.(*RpbGetResp), nil
}

// FetchObject returns an object from a bucket and returns a RpbGetResp response.
func (c *Client) FetchObject(bucket, key string) (*RpbGetResp, error) {
	return c.fetchObject(nil, bucket, key)
}

// NewStoreObjectRequest prepares a StoreObject request.
func (c *Client) NewStoreObjectRequest(bucket, key string) *RpbPutReq {
	return &RpbPutReq{
		Bucket: []byte(bucket),
		Key:    []byte(key),
	}
}

func (c *Client) storeObject(opts *RpbPutReq, bucket, key string, in interface{}) (*RpbPutResp, error) {
	if opts == nil {
		opts = c.NewStoreObjectRequest(bucket, key)
	}

	if _, ok := in.(*RpbContent); ok {
		opts.Content = in.(*RpbContent)
	} else {
		// Determine the primitive type of content.
		t := reflect.TypeOf(in)

		switch t.Kind() {
		case reflect.String:
			opts.Content = &RpbContent{
				Value:       []byte(in.(string)),
				ContentType: []byte("plain/text"),
			}
			break
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			opts.Content = &RpbContent{
				Value:       []byte(strconv.FormatInt(int64(in.(int)), 10)),
				ContentType: []byte("plain/text"),
			}
			break
		default:
			opts.Content = &RpbContent{
				Value:       in.([]byte),
				ContentType: []byte("application/octet-stream"),
			}
			break
		}
	}

	if opts.Content == nil {
		return nil, errors.New("Invalid content type passed.  Must be RpbContent, string, int, or []byte")
	}

	response, err := c.ReqResp(opts, "RpbPutReq", false)
	if err != nil {
		return nil, err
	}

	return response.(*RpbPutResp), nil
}

// StoreObject puts an object with key into bucket and returns a RpbGetResp response.
//
// The `in` content can be passed as either a RpbContent, string, int, or []byte.
//
// Use RpbContent if you need absolute control over what is going into Riak.
func (c *Client) StoreObject(bucket, key string, in interface{}) (*RpbPutResp, error) {
	return c.storeObject(nil, bucket, key, in)
}

// NewDeleteObjectRequest prepares a DeleteObject request.
func (c *Client) NewDeleteObjectRequest(bucket, key string) *RpbDelReq {
	return &RpbDelReq{
		Bucket: []byte(bucket),
		Key:    []byte(key),
	}
}

func (c *Client) deleteObject(opts *RpbDelReq, bucket, key string) ([]byte, error) {
	if opts == nil {
		opts = c.NewDeleteObjectRequest(bucket, key)
	}

	response, err := c.ReqResp(opts, "RpbDelReq", false)
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}

// DeleteObject removes object with key from bucket.
func (c *Client) DeleteObject(bucket, key string) ([]byte, error) {
	return c.deleteObject(nil, bucket, key)
}

// NewFetchStructRequest prepares a FetchStruct request.
func (c *Client) NewFetchStructRequest(bucket, key string) *RpbGetReq {
	return &RpbGetReq{
		Bucket: []byte(bucket),
		Key:    []byte(key),
	}
}

func (c *Client) fetchStruct(opts *RpbGetReq, bucket, key string, out interface{}) (*RpbGetResp, error) {
	if c.Coder == nil {
		panic("Cannot fetch to a struct unless a coder has been set")
	}

	if opts == nil {
		opts = c.NewFetchStructRequest(bucket, key)
	}

	response, err := c.ReqResp(opts, "RpbGetReq", false)
	if err != nil {
		return &RpbGetResp{}, err
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
				return &RpbGetResp{}, err
			}
		default:
			panic("Invalid out struct type passed to FetchStruct")
		}
	}

	return response.(*RpbGetResp), nil
}

// FetchStruct returns an object from a bucket and unmarshals it into the passed struct.
//
// Pass RpbGetReq to SetOpts for optional parameters.
func (c *Client) FetchStruct(bucket, key string, out interface{}) (*RpbGetResp, error) {
	return c.fetchStruct(nil, bucket, key, out)
}

// NewStoreStructRequest prepares a StoreStruct request.
func (c *Client) NewStoreStructRequest(bucket, key string) *RpbPutReq {
	return &RpbPutReq{
		Bucket: []byte(bucket),
		Key:    []byte(key),
	}
}

func (c *Client) storeStruct(opts *RpbPutReq, bucket, key string, in interface{}) (*RpbPutResp, error) {
	if c.Coder == nil {
		panic("Cannot store a struct unless a coder has been set")
	}

	if opts == nil {
		opts = c.NewStoreStructRequest(bucket, key)
	}

	if _, ok := in.(*RpbContent); ok {
		opts.Content = in.(*RpbContent)
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
				opts.Content = encctnt
				break
			default:
				panic("Invalid in struct type passed to StoreStruct")
			}
		}
	}

	if opts.Content == nil {
		return nil, errors.New("Invalid content type passed.  Must be struct, RpbContent, string, int, or []byte")
	}

	response, err := c.ReqResp(opts, "RpbPutReq", false)
	if err != nil {
		return nil, err
	}

	return response.(*RpbPutResp), nil
}

// StoreStruct marshals the data from a struct, and adds it with key into bucket.
//
// Check Coder.Marshall() for `riak` tags that can be set on a structure for automated indexes and links.
func (c *Client) StoreStruct(bucket, key string, in interface{}) (*RpbPutResp, error) {
	return c.storeStruct(nil, bucket, key, in)
}
