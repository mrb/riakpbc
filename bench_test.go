package riakpbc

import (
	"testing"
)

func BenchmarkReadSync(b *testing.B) {
	b.StopTimer()
	conn := New([]string{"127.0.0.1:8087", "127.0.0.1:8088"})
	conn.Dial()
	conn.StoreObject("bucket", "key", &Data{Data: "rules"})

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, _ = conn.FetchObject("bucket", "key")
	}
}

func BenchmarkReadAsync(b *testing.B) {
	b.StopTimer()
	conn := New([]string{"127.0.0.1:8087", "127.0.0.1:8088"})
	conn.Dial()
	conn.StoreObject("bucket", "key", &Data{Data: "rules"})

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

func BenchmarkStoreStruct(b *testing.B) {
	b.StopTimer()
	conn := New([]string{"127.0.0.1:8087", "127.0.0.1:8088"})
	conn.Dial()
	conn.StoreObject("bucket", "key", &Data{Data: "rules"})

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, _ = conn.FetchObject("bucket", "key")
	}
}

func BenchmarkStoreRpbContent(b *testing.B) {
	b.StopTimer()
	conn := New([]string{"127.0.0.1:8087", "127.0.0.1:8088"})
	conn.Dial()

	data := &RpbContent{
		Value:       []byte("{\"data\":\"rules\"}"),
		ContentType: []byte("application/json"),
	}
	conn.StoreObject("bucket", "key", data)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, _ = conn.FetchObject("bucket", "key")
	}
}
