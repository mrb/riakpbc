package riakpbc

import (
	"mrb/riakpbc"
	"testing"
)

func BenchmarkRead(b *testing.B) {
	b.N = 10000
	riak, err := riakpbc.Dial("127.0.0.1:8081")

	if err != nil {
		return
	}

	for i := 0; i < b.N; i++ {
		_, _ = riak.FetchObject("bucket", "keyzles")
	}
}
