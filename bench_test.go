package riakpbc

import (
	"testing"
)

func BenchmarkReadSync(b *testing.B) {
	b.StopTimer()
	conn, err := New("127.0.0.1:8087", 1e8, 1e8)

	if err != nil {
		return
	}
	conn.Dial()
	conn.StoreObject("bucket", "key", "{}")

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, _ = conn.FetchObject("bucket", "key")
	}
}

func BenchmarkReadAsync(b *testing.B) {
	b.StopTimer()
	conn, err := New("127.0.0.1:8087", 1e8, 1e8)
	if err != nil {
		return
	}
	conn.Dial()
	conn.StoreObject("bucket", "key", "{}")

	ch := make(chan []byte, 1)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		go func(c *Conn) {
			data, _ := c.FetchObject("bucket", "key")
			select {
			case ch <- data:
			default:
			}
		}(conn)
	}

	for i := 0; i < b.N; i++ {
		_ = <-ch
	}
}
