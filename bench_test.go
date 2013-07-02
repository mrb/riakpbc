package riakpbc

import (
	"testing"
)

func BenchmarkReadSync(b *testing.B) {
	b.StopTimer()
	client := NewClient([]string{"127.0.0.1:8087", "127.0.0.1:8088"}, nil)
	client.Dial()
	session := client.Session()
	session.StoreObject("bucket", "key", &Data{Data: "rules"})

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, _ = session.FetchObject("bucket", "key")
	}
}

func BenchmarkReadAsync(b *testing.B) {
	b.StopTimer()
	client := NewClient([]string{"127.0.0.1:8087", "127.0.0.1:8088"}, nil)
	client.Dial()
	session := client.Session()
	session.StoreObject("bucket", "key", &Data{Data: "rules"})

	ch := make(chan bool, b.N)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		go func(node *Node) {
			session := client.Session()
			_, _ = node.FetchObject("bucket", "key")
			client.Free(session)
			select {
			case ch <- true:
			default:
			}
		}(session)
	}

	for i := 0; i < b.N; i++ {
		<-ch
	}
}

func BenchmarkStoreStruct(b *testing.B) {
	b.StopTimer()
	client := NewClient([]string{"127.0.0.1:8087", "127.0.0.1:8088"}, nil)
	client.Dial()
	session := client.Session()
	session.StoreObject("bucket", "key", &Data{Data: "rules"})

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, _ = session.FetchObject("bucket", "key")
	}
}

func BenchmarkStoreRpbContent(b *testing.B) {
	b.StopTimer()
	client := NewClient([]string{"127.0.0.1:8087", "127.0.0.1:8088"}, nil)
	client.Dial()
	session := client.Session()

	data := &RpbContent{
		Value:       []byte("{\"data\":\"rules\"}"),
		ContentType: []byte("application/json"),
	}
	session.StoreObject("bucket", "key", data)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, _ = session.FetchObject("bucket", "key")
	}
}
