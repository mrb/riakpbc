package main

import (
	"github.com/mrb/riakpbc"
	"log"
	"time"
)

type Data struct {
	Data string `json:"data"`
}

func main() {
	cluster := []string{"127.0.0.1:8087", "127.0.0.1:8088", "127.0.0.1:8089", "127.0.0.1:8090", "127.0.0.1:1030007"}
	riak := riakpbc.New(cluster)

	err := riak.Dial()
	if err != nil {
		log.Print(err)
	}

	actionBegin := time.Now()
	var actionEnd time.Time

	data := "{'data':'rules', 'data':'rules', 'data':'rules','data':'rules', 'data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules'}"

	var times int
	for {
		times = times + 1

		_, err = riak.StoreObject("bucket", "data", &Data{Data: data})
		_, err = riak.SetClientId("coolio")

		id, err := riak.GetClientId()
		if err != nil {
			log.Print("1 [ERR] ", err)
			break
		}

		log.Print("[OK] ", id)

		resp, err := riak.FetchObject("bucket", "data")
		if err != nil {
			log.Print("2 [ERR] ", err)
			break
		}

		log.Print("[OK] ", len(resp.GetContent()[0].GetValue()))

		_, err = riak.StoreObject("bucket", "moreData", "stringData")
		if err != nil {
			log.Print("3 [ERR] ", err)
			break
		}

		resp, err = riak.FetchObject("bucket", "moreData")
		if err != nil {
			log.Print("4 [ERR] ", err)
			break
		}

		log.Print("[OK] ", len(resp.GetContent()[0].GetValue()))
		log.Print("Iteration ", times)
	}

	actionEnd = time.Now()
	actionDuration := actionEnd.Sub(actionBegin)
	log.Print("Ran for ", actionDuration)

	riak.Close()
}
