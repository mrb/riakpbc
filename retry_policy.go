package riakpbc

import "log"

type NetworkReadFunc func() (data []byte, err error)
type NetworkWriteFunc func(data []byte) (err error)

type RetryPolicy interface {
	Read(readfunc NetworkReadFunc) (data []byte, err error)
	Write(writefunc NetworkWriteFunc, data []byte) (err error)
}

type SimpleRetrier struct {
	retries int
}

func NewSimpleRetryPolicy(retries int) *SimpleRetrier {
	return &SimpleRetrier{
		retries: retries,
	}
}

func (retrier *SimpleRetrier) Read(readfunc NetworkReadFunc) ([]byte, error) {
	var err error
	var data []byte

	for try := 0; try < retrier.retries; try++ {
		data, err = readfunc()
		log.Print("[RETRY] Attempt ", try, " of ", retrier.retries)
	}

	return data, err
}

func (retrier *SimpleRetrier) Write(writefunc NetworkWriteFunc, data []byte) error {
	var err error

	for try := 0; try < retrier.retries; try++ {
		err = writefunc(data)
		log.Print("[RETRY] Attempt ", try, " of ", retrier.retries)
	}

	return err
}
