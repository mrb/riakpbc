package main

import (
	"github.com/mrb/riakpbc"
	"log"
)

type Data struct {
	Data string `json:"data"`
}

func main() {
	cluster := []string{"127.0.0.1:10017", "127.0.0.1:10027", "127.0.0.1:1037"}
	riak := riakpbc.New(cluster)

	err := riak.Dial()
	if err != nil {
		log.Print(err)
	}

	data := "{'data':'rules', 'data':'rules', 'data':'rules','data':'rules', 'data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules'}"

	_, err = riak.StoreObject("bucket", "data", &Data{Data: data})
	_, err = riak.SetClientId("coolio")

	id, err := riak.GetClientId()
	log.Print(id, " - ", err)

	resp, err := riak.FetchObject("bucket", "data")
	log.Print(len(resp.GetContent()[0].GetValue()), err)

	_, err = riak.StoreObject("bucket", "moreData", "stringData")
	resp, err = riak.FetchObject("bucket", "moreData")
	log.Print(len(resp.GetContent()[0].GetValue()), " - ", err)

	riak.Close()
}
