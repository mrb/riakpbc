package riakpbc

import (
	"encoding/json"
	"testing"
)

type EncodeData struct {
	Email   string `json:"email" riak:"index"`
	Twitter string `json:"twitter" riak:"index"`
}

func TestEncode(t *testing.T) {
	e := NewEncoder()

	data := &EncodeData{
		Email:   "riak@example.com",
		Twitter: "riak-twitter",
	}

	encdata, err := e.Marshal(data)
	if err != nil {
		t.Error(err.Error())
	}

	key := string(encdata.GetIndexes()[0].GetKey())
	if key != "email_bin" {
		t.Errorf("Expected email_bin, got %s", key)
	}

	jsondata, err := json.Marshal(data)
	if err != nil {
		t.Error(err.Error())
	}
	if string(encdata.GetValue()) != string(jsondata) {
		t.Errorf("Expected %s, got %s", string(encdata.GetValue()), string(jsondata))
	}

	//t.Log(encdata.GetIndexes())
	//t.Log(string(encdata.GetValue()))
}
