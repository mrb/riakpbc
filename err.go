package riakpbc

import (
	"errors"
)

var (
	ErrLengthZero     = errors.New("length response 0")
	ErrCorruptHeader  = errors.New("corrupt header")
	ErrObjectNotFound = errors.New("object not found")
	ErrNoSuchCommand  = errors.New("no such command")
	ErrBucketExists   = errors.New("bucket exists")
	ErrRiakError      = errors.New("riak error")
	ErrNotDone        = errors.New("not done")
	ErrReadTimeout    = errors.New("read timeout")
	ErrWriteTimeout   = errors.New("write timeout")
	ErrZeroNodes      = errors.New("zero nodes in pool")
	ErrNoContent      = errors.New("no content")
	ErrAllNodesDown   = errors.New("all nodes down")
)
