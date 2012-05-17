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

	obj, err := riak.FetchObject("buckest", "key")

	if err != nil {
		log.Print(err)
	} else {
		log.Printf("%s", pretty.Formatter(obj))
	}

	bux, err := riak.ListBuckets()

	if err != nil {
		log.Print(err)
	} else {
		log.Printf("%s", pretty.Formatter(bux))
	}

	info, err := riak.GetServerInfo()

	if err != nil {
		log.Print(err)
	} else {
		log.Printf("%s", pretty.Formatter(info))
	}

	storeresp, _ := riak.StoreObject("bucket", "keyzles", "{'keyzle':'deyzle'}")
	if err != nil {
		log.Print(err)
	} else {
		log.Printf("%s", pretty.Formatter(storeresp))
	}

	nobj, err := riak.FetchObject("bucket", "keyzles")

	if err != nil {
		log.Print(err)
	} else {
		log.Printf("%s", pretty.Formatter(nobj))
	}

	nval := uint32(1)
	allowmult := false

	nobj, err = riak.SetBucket("squadronsz", &nval, &allowmult)

	if err != nil {
		log.Print(err)
	} else {
		log.Printf("%s", pretty.Formatter(nobj))
	}

	storeresp, err = riak.StoreObject("squadronsz", "nsymets", "{'zzzzlayers':['deyzle','freyzle','chezyle']}")

	if err != nil {
		log.Print(err)
	} else {
		log.Printf("%s", pretty.Formatter(storeresp))
	}

	obj, err = riak.FetchObject("squadronsz", "nsymets")

	if err != nil {
		log.Print(err)
	} else {
		log.Printf("%s", pretty.Formatter(obj))
	}

	riak.Close()
}
