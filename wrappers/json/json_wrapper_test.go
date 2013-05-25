package json

import (
	"github.com/mrb/riakpbc"
	"testing"
)

type WrapperTestStruct struct {
	Data string `json:"data"`
}

func setupConnection(t *testing.T) (conn *riakpbc.Conn) {
	conn, err := riakpbc.New("127.0.0.1:8087", 1e8, 1e8)
	if err != nil {
		t.Error(err.Error())
	}
	if err := conn.Dial(); err != nil {
		t.Error(err.Error())
	}
	return
}

func TestFindOne(t *testing.T) {
	conn := setupConnection(t)
	if _, err := conn.StoreObject("riakpbctestbucket", "testkey", []byte("{\"data\":\"is awesome!\"}"), "application/json"); err != nil {
		t.Error(err.Error())
	}

	w := NewJsonWrapper(conn)

	data := WrapperTestStruct{}
	if err := w.Bucket("riakpbctestbucket").Find("testkey").One(&data); err != nil {
		t.Error(err.Error())
	}

	if data.Data != "is awesome!" {
		t.Errorf("Expected %s, got %s", "is awesome!", data.Data)
	}

	if _, err := conn.DeleteObject("riakpbctestbucket", "testkey"); err != nil {
		t.Error(err.Error())
	}

	if err := w.Bucket("").Find("inserttestkey").One(&data); err != nil {
		if err.Error() != "no bucket set" {
			t.Error(err.Error())
		}
	}

	if err := w.Bucket("riakpbctestbucket").Find("").One(&data); err != nil {
		if err.Error() != "no key set" {
			t.Error(err.Error())
		}
	}
}

func TestInsert(t *testing.T) {
	conn := setupConnection(t)

	w := NewJsonWrapper(conn)

	data := WrapperTestStruct{
		Data: "TestInsert",
	}
	if err := w.Bucket("riakpbctestbucket").Insert("inserttestkey", data); err != nil {
		t.Error(err.Error())
	}

	check := WrapperTestStruct{}
	if err := w.Bucket("riakpbctestbucket").Find("inserttestkey").One(&check); err != nil {
		t.Error(err.Error())
	}

	if check.Data != data.Data {
		t.Errorf("Expected %s, got %s", data.Data, check.Data)
	}

	if err := w.Bucket("").Insert("inserttestkey", data); err != nil {
		if err.Error() != "no bucket set" {
			t.Error(err.Error())
		}
	}
}

func TestDelete(t *testing.T) {
	conn := setupConnection(t)

	w := NewJsonWrapper(conn)

	if err := w.Bucket("riakpbctestbucket").Delete("inserttestkey"); err != nil {
		t.Error(err.Error())
	}

	check := WrapperTestStruct{}
	if err := w.Bucket("riakpbctestbucket").Find("inserttestkey").One(&check); err != nil {
		if err.Error() != "object not found" {
			t.Error(err.Error())
		}
	}

	if err := w.Bucket("").Delete("inserttestkey"); err != nil {
		if err.Error() != "no bucket set" {
			t.Error(err.Error())
		}
	}
}

func ExampleJsonWrapper() {
	conn, err := riakpbc.New("127.0.0.1:8087", 1e8, 1e8)
	if err != nil {
		panic(err)
	}
	if err := conn.Dial(); err != nil {
		panic(err)
	}

	w := NewJsonWrapper(conn)

	// Insert
	data := WrapperTestStruct{
		Data: "ExampleInsert",
	}
	if err := w.Bucket("databucket").Insert("datakey", data); err != nil {
		// handle err
	}

	// Update
	data.Data = "ExampleInsertChange"
	if err := w.Bucket("databucket").Update("datakey", data); err != nil {
		// handle err
	}

	// Find
	find := WrapperTestStruct{}
	if err := w.Bucket("databucket").Find("datakey").One(&find); err != nil {
		// handle err
	}

	// Delete
	if err := w.Bucket("databucket").Delete("datakey"); err != nil {
		// handle err
	}

	conn.Close()
}
