package main

import (
	"github.com/mrb/riakpbc"
	"log"
	"runtime"
	"time"
)

type Data struct {
	Data string `json:"data"`
}

func main() {
	runtime.GOMAXPROCS(4)
	cluster := []string{"127.0.0.1:8087", "127.0.0.1:8088", "127.0.0.1:8089", "127.0.0.1:8090"}
	riak := riakpbc.New(cluster)

	err := riak.Dial()
	if err != nil {
		log.Print(err)
	}

	var actionEnd time.Time
	actionBegin := time.Now()

	c := make(chan int)

	for g := 0; g < 4; g++ {
		go func(which int) {
			log.Print("<", which, "> Loaded")
			var times int
			for {
				actionBegin := time.Now()

				times = times + 1
				riak.StoreObject("bucket", "data", "{'ok':'ok'}")
				riak.SetClientId("coolio")
				riak.GetClientId()
				data, err := riak.FetchObject("bucket", "data")
				if err != nil {
					break
				}
				if string(data.GetContent()[0].GetValue()) != "{'ok':'ok'}" {
					log.Fatal("FUCK")
				}
				riak.StoreObject("bucket", "moreData", "stringData")
				riak.FetchObject("bucket", "moreData")

				actionDuration := time.Now().Sub(actionBegin)
				log.Print("<", which, "> @", times, " ", riak.Pool(), " ", actionDuration)
			}
		}(g)
	}
	<-c
	actionEnd = time.Now()
	actionDuration := actionEnd.Sub(actionBegin)
	log.Print("Ran for ", actionDuration)

	riak.Close()
}
