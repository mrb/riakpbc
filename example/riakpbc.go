package main

import (
	"github.com/kr/pretty"
	"log"
	"mrb/riakpbc"
)

func main() {
	riak, err := riakpbc.Dial("127.0.0.1:8087")

	if err != nil {
		log.Print(err)
		return
	}

	riak.Close()
}
