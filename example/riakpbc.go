package main

import (
	"log"
	"mrb/riakpbc"
  "time"
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

	ok, err := riak.StoreObject("buckey", "bro", "{'data':'rules'}")
	log.Print(string(ok), " - ", err)

	ok, err = riak.SetClientId("coolio")
	log.Print(string(ok), " - ", err)

	ok, err = riak.GetClientId()
	log.Print(string(ok), " - ", err)

	obj, err := riak.FetchObject("buckey", "bro")
	log.Print(string(obj), " - ", err)

	ch := make(chan []byte, 1)

	log.Print("start async")

	for i := 0; i < 10; i++ {
		go func() {
			data, _ := riak.FetchObject("buckey", "bro")
			select {
			case ch <- data:
			default:
			}
		}()

	}

	for i := 0; i < 10; i++ {
		_ = <-ch
	}
	log.Print("done async")

	log.Print("start sync")
	for i := 0; i < 1000; i++ {
		_, _ = riak.FetchObject("buckey", "bro")
	}
	log.Print("done sync")

	riak.Close()
}
