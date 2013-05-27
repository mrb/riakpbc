package riakpbc

import (
	"encoding/json"
	"log"
	"testing"
)

func BenchmarkReadSync(b *testing.B) {
	b.StopTimer()
	conn := New([]string{"127.0.0.1:8087", "127.0.0.1:8088"})
	conn.Dial()

	data, err := json.Marshal(&Data{Data: "rules"})
	if err != nil {
		log.Println(err.Error())
	}
	content := &RpbContent{
		Value:       data,
		ContentType: []byte("application/json"),
	}
	conn.StoreObject("bucket", "key", content)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, _ = conn.FetchObject("bucket", "key")
	}
}

func BenchmarkReadAsync(b *testing.B) {
	b.StopTimer()
	conn := New([]string{"127.0.0.1:8087", "127.0.0.1:8088"})
	conn.Dial()

	data, err := json.Marshal(&Data{Data: "rules"})
	if err != nil {
		log.Println(err.Error())
	}
	content := &RpbContent{
		Value:       data,
		ContentType: []byte("application/json"),
	}
	conn.StoreObject("bucket", "key", content)

	ch := make(chan bool, b.N)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		go func(c *Conn) {
			_, _ = c.FetchObject("bucket", "key")
			select {
			case ch <- true:
			default:
			}
		}(conn)
	}

	for i := 0; i < b.N; i++ {
		<-ch
	}
}
