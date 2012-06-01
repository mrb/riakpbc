package main

import (
	//"github.com/kr/pretty"
	"log"
	"mrb/riakpbc"
)

func main() {
	riak, err := riakpbc.New("127.0.0.1:8087", 1e8, 1e8)

	if err != nil {
		log.Print(err)
		return
	}

  riak.Dial()
  ok, err := riak.GetClientId()

  log.Print(ok, err)

  ok, err = riak.StoreObject("buckey", "bro", "{}")

  log.Print(ok, err)

  ok, err = riak.SetClientId("coolio")

  log.Print(ok, err)

	riak.Close()
}
