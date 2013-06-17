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
	cluster := []string{"127.0.0.1:8087", "127.0.0.1:8088", "127.0.0.1:8089", "127.0.0.1:8090", "127.0.0.1:1030007"}
	riak := riakpbc.New(cluster)

	err := riak.Dial()
	if err != nil {
		log.Print(err)
	}

	actionBegin := time.Now()
	var actionEnd time.Time

	data := "{'data':'rules', 'data':'rules', 'data':'rules','data':'rules', 'data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules','data':'rules', 'data':'rules', 'data':'rules','data':'rules'}"
	c := make(chan int)
	for g := 0; g < 2; g++ {
		go func(which int, stuff string) {
			log.Print(which)
			var times int
			for {
				times = times + 1
				riak.SelectNode()
				log.Print(riak.Current())
			}
		}(g, data)
	}
	log.Print("DONE")
	<-c
	actionEnd = time.Now()
	actionDuration := actionEnd.Sub(actionBegin)
	log.Print("Ran for ", actionDuration)

	riak.Close()
}
