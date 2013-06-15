package riakpbc

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// typeOfBytes is a special check against Slices of []byte.
var typeOfBytes = reflect.TypeOf([]byte(nil))

// MarshalMethod is the method signature of a marshaller.
type MarshalMethod func(interface{}, *RpbContent) error

type Coder struct {
	tag        string        // the tag to match for the marshaller
	marshaller MarshalMethod // the method to run on the data
}

// JsonMarshaller is an example of a MarshalMethod that is passed to NewEncode().
//
// If a different data marshaller is desired, such as XML, YAML, etc., use this as a template.
func JsonMarshaller(data interface{}, out *RpbContent) error {
	jsondata, err := json.Marshal(data)
	if err != nil {
		return err
	}
	out.Value = jsondata
	out.ContentType = []byte("application/json")
	return nil
}

// NewCoder requires a tag and MarshalMethod.
func NewCoder(tag string, marshaller MarshalMethod) *Coder {
	self := new(Coder)
	self.tag = tag
	self.marshaller = marshaller
	return self
}

// Marshal takes a struct with `riak` tagged fields and builds the correct
// RpbContent to send along to Riak.
//
// Any fields of type string are set as a _bin index, and fields of any
// int type set to an _int index.
//
// Examples:
//
//  // Field is a _bin index
//  Field string `riak:"index"`
//
//  // Field is an _int index
//  Field int `riak:"index"`
//
//  // Field is a _bin index and also a json field in the actual data.
//  Field string `json:"field" riak:"index"`
func (self *Coder) Marshal(data interface{}) (*RpbContent, error) {
	t := reflect.ValueOf(data)
	if t.Kind() != reflect.Ptr {
		return nil, errors.New(fmt.Sprintf("Expected a pointer not %s", t.Kind()))
	}

	// Output
	out := &RpbContent{}

	e := t.Elem()
	switch e.Kind() {
	case reflect.Struct:
		matched := false

		for i := 0; i < e.NumField(); i++ {
			val := e.Field(i).Interface()
			fld := e.Type().Field(i)
			knd := e.Field(i).Kind()
			tag := fld.Tag

			// Skip anonymous fields
			if fld.Anonymous {
				continue
			}

			// Match marshaller tag
			if matched == false && tag.Get(self.tag) != "" {
				matched = true
			}

			if tdata := tag.Get("riak"); tdata != "" {
				for _, tfield := range strings.Split(tdata, ",") {
					switch tfield {
					case "index":
						index := &RpbPair{}
						var key string
						switch knd {
						case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
							key = fld.Name + "_int"
							switch knd {
							case reflect.Int:
								buf := make([]byte, 10)
								binary.PutVarint(buf, int64(val.(int)))
								index.Value = buf
								break
							case reflect.Int8:
								buf := make([]byte, 2)
								binary.PutVarint(buf, int64(val.(int8)))
								index.Value = buf
								break
							case reflect.Int16:
								buf := make([]byte, 3)
								binary.PutVarint(buf, int64(val.(int16)))
								index.Value = buf
								break
							case reflect.Int32:
								buf := make([]byte, 5)
								binary.PutVarint(buf, int64(val.(int32)))
								index.Value = buf
								break
							case reflect.Int64:
								buf := make([]byte, 10)
								binary.PutVarint(buf, int64(val.(int64)))
								index.Value = buf
								break
							}
							index.Key = []byte(strings.ToLower(key))
							break
						case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
							key = fld.Name + "_int"
							switch knd {
							case reflect.Uint:
								buf := make([]byte, 10)
								binary.PutUvarint(buf, uint64(val.(uint)))
								index.Value = buf
								break
							case reflect.Uint8:
								buf := make([]byte, 2)
								binary.PutUvarint(buf, uint64(val.(uint8)))
								index.Value = buf
								break
							case reflect.Uint16:
								buf := make([]byte, 3)
								binary.PutUvarint(buf, uint64(val.(uint16)))
								index.Value = buf
								break
							case reflect.Uint32:
								buf := make([]byte, 5)
								binary.PutUvarint(buf, uint64(val.(uint32)))
								index.Value = buf
								break
							case reflect.Uint64:
								buf := make([]byte, 10)
								binary.PutUvarint(buf, uint64(val.(uint64)))
								index.Value = buf
								break
							}
							index.Key = []byte(strings.ToLower(key))
							break
						case reflect.String:
							key = fld.Name + "_bin"
							index.Key = []byte(strings.ToLower(key))
							index.Value = []byte(val.(string))
							break
						case reflect.Slice:
							if fld.Type == typeOfBytes {
								key = fld.Name + "_bin"
								index.Key = []byte(strings.ToLower(key))
								index.Value = []byte(val.([]byte))
							}
							break
						}
						out.Indexes = append(out.Indexes, index)
						break
					case "link":

						break
					}
				}
			}
		}

		// Automatically marshal structures
		if matched {
			if err := self.marshaller(&data, out); err != nil {
				return nil, err
			}
		}
		break
	default:
		return nil, errors.New("Marshal expected a struct")
	}

	return out, nil
}
