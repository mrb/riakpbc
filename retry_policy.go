package riakpbc

import (
	"log"
	"time"
)

type NetworkReadFunc func() (data []byte, err error)
type NetworkWriteFunc func(data []byte) (err error)

type RetryPolicy interface {
	Read(readfunc NetworkReadFunc) (data []byte, err error)
	Write(writefunc NetworkWriteFunc, data []byte) (err error)
}

type SimpleRetrier struct {
	retries      int
	retrySleepMS int
}

func NewSimpleRetryPolicy(retries, retrySleepMS int) *SimpleRetrier {
	return &SimpleRetrier{
		retries:      retries,
		retrySleepMS: retrySleepMS,
	}
}

func (retrier *SimpleRetrier) Read(readfunc NetworkReadFunc) ([]byte, error) {
	data, err := readfunc()

	if err != nil {
		for try := 0; try < retrier.retries; try++ {
			time.Sleep(time.Duration(retrier.retrySleepMS) * time.Millisecond)
			log.Print("[RETRY] Attempt ", try, " of ", retrier.retries)
			data, err = readfunc()
			if err == nil {
				return data, err
			}
		}
	}

	return data, err
}

func (retrier *SimpleRetrier) Write(writefunc NetworkWriteFunc, data []byte) error {
	err := writefunc(data)

	if err != nil {
		for try := 0; try < retrier.retries; try++ {
			time.Sleep(time.Duration(retrier.retrySleepMS) * time.Millisecond)
			log.Print("[RETRY] Attempt ", try, " of ", retrier.retries)
			err = writefunc(data)
			if err == nil {
				return err
			}
		}
	}

	return err
}
