package riakpbc

import (
	"testing"
)

func BenchmarkReadSync(b *testing.B) {
	b.StopTimer()
	client := NewClient([]string{"127.0.0.1:8087", "127.0.0.1:8088"})
	client.Dial()
	client.StoreObject("bucket", "key", &Data{Data: "rules"})

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, _ = client.FetchObject("bucket", "key")
	}
}

func BenchmarkReadAsync(b *testing.B) {
	b.StopTimer()
	client := NewClient([]string{"127.0.0.1:8087", "127.0.0.1:8088"})
	client.Dial()
	client.StoreObject("bucket", "key", &Data{Data: "rules"})

	ch := make(chan bool, b.N)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		go func(c *Client) {
			_, _ = c.FetchObject("bucket", "key")
			select {
			case ch <- true:
			default:
			}
		}(client)
	}

	for i := 0; i < b.N; i++ {
		<-ch
	}
}

func BenchmarkStoreStruct(b *testing.B) {
	b.StopTimer()
	client := NewClient([]string{"127.0.0.1:8087", "127.0.0.1:8088"})
	client.Dial()
	client.StoreObject("bucket", "key", &Data{Data: "rules"})

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, _ = client.FetchObject("bucket", "key")
	}
}

func BenchmarkStoreRpbContent(b *testing.B) {
	b.StopTimer()
	client := NewClient([]string{"127.0.0.1:8087", "127.0.0.1:8088"})
	client.Dial()

	data := &RpbContent{
		Value:       []byte("{\"data\":\"rules\"}"),
		ContentType: []byte("application/json"),
	}
	client.StoreObject("bucket", "key", data)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, _ = client.FetchObject("bucket", "key")
	}
}
