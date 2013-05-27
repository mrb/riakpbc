package riakpbc

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type Encoder struct{}

func NewEncoder() *Encoder {
	self := new(Encoder)
	return self
}

func (self *Encoder) Marshal(data interface{}) (*RpbContent, error) {
	t := reflect.ValueOf(data)
	if t.Kind() != reflect.Ptr {
		return nil, errors.New(fmt.Sprintf("Expected a pointer not %s", t.Kind()))
	}

	// Output
	isJson := false
	out := &RpbContent{}

	e := t.Elem()
	switch e.Kind() {
	case reflect.Struct:
		for i := 0; i < e.NumField(); i++ {
			val := e.Field(i).Interface()
			fld := e.Type().Field(i)
			knd := e.Field(i).Kind()
			tag := fld.Tag

			// Skip anonymous fields
			if fld.Anonymous {
				continue
			}

			// If any of the struct tags are "json" then this is a json structure.
			if tag.Get("json") != "" {
				isJson = true
			}

			if tdata := tag.Get("riak"); tdata != "" {
				for _, tfield := range strings.Split(tdata, ",") {
					switch tfield {
					case "index":
						var key string
						switch knd {
						case reflect.Int:
							key = fld.Name + "_int"
							break
						case reflect.String:
							key = fld.Name + "_bin"
							break
						}
						index := &RpbPair{
							Key:   []byte(strings.ToLower(key)),
							Value: []byte(val.(string)),
						}
						out.Indexes = append(out.Indexes, index)
						break
					case "link":

						break
					}
				}
			}
		}
		break
	default:
		return nil, errors.New("Marshal expected a struct")
	}

	// Automatically marshal json structures.
	if isJson {
		jsondata, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		out.Value = jsondata
		out.ContentType = []byte("application/json")
	}

	return out, nil
}
