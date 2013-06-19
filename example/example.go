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
	cluster := []string{"127.0.0.1:8087", "127.0.0.1:8088"}
	riak := riakpbc.New(cluster)

	err := riak.Dial()
	if err != nil {
		log.Print(err)
	}

	actionBegin := time.Now()
	var actionEnd time.Time

	c := make(chan int)

	for g := 0; g < 2; g++ {
		go func(which int) {
			log.Print(which)
			var times int
			for {
				times = times + 1
				riak.StoreObject("bucket", "data", "{'ok':'ok'}")
				//riak.SetClientId("coolio")
				//riak.GetClientId()
				riak.FetchObject("bucket", "data")
				//riak.StoreObject("bucket", "moreData", "stringData")
				riak.FetchObject("bucket", "moreData")

				actionEnd = time.Now()
				actionDuration := actionEnd.Sub(actionBegin)

				log.Print("gr: ", which, " ", times, " Nodes: ", riak.Pool(), actionDuration)
				time.Sleep(20 * time.Millisecond)
			}
		}(g)
	}
	log.Print("DONE")
	<-c
	actionEnd = time.Now()
	actionDuration := actionEnd.Sub(actionBegin)
	log.Print("Ran for ", actionDuration)

	riak.Close()
}
