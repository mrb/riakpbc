package main

import (
	"log"
	"github.com/mrb/riakpbc"
)

func main() {
	riak, err := riakpbc.New("127.0.0.1:8087", 1e8, 1e8)

	if err != nil {
		log.Print(err)
		return
	}

	err = riak.Dial()
	if err != nil {
		log.Print(err)
		return
	}

	data := []byte("{'data':'rules', 'data':'rules', 'data':'rules','data':'rules', 'data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules' }")

	ok, err := riak.StoreObject("bucket", "data", data, "application/json")
	log.Print(string(ok), " - ", err)

	ok, err = riak.SetClientId("coolio")
	log.Print(string(ok), " - ", err)

	ok, err = riak.GetClientId()
	log.Print(string(ok), " - ", err)

	obj, err := riak.FetchObject("bucket", "data")
	log.Print(string(obj), " - ", err)

	riak.Close()
}
