package riakpbc

import (
	"testing"
)

func BenchmarkReadSync(b *testing.B) {
	b.StopTimer()
	conn := New([]string{"127.0.0.1:8087", "127.0.0.1:8088"})
	conn.Dial()
	conn.StoreObject("bucket", "key", []byte("{}"), "application/json", nil)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, _ = conn.FetchObject("bucket", "key", nil)
	}
}

func BenchmarkReadAsync(b *testing.B) {
	b.StopTimer()
	conn := New([]string{"127.0.0.1:8087", "127.0.0.1:8088"})
	conn.Dial()
	conn.StoreObject("bucket", "key", []byte("{}"), "application/json", nil)

	ch := make(chan bool, b.N)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		go func(c *Conn) {
			_, _ = c.FetchObject("bucket", "key", nil)
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
