package riakpbc

import (
	"math"
	"sync"
	"time"
)

// Thanks to @aphyr and @seancribbs for their work on:
// https://github.com/basho/riak-ruby-client
// Which directly inspired this implementation
type Decaying struct {
	p  float64
	e  float64
	r  float64
	t0 time.Time
	sync.Mutex
}

// NewDecaying returns a new decaying error rate object - the value of `p`
// reduces, be default 50% every 10 seconds. This gives us a nice, tunable
// way to control our interactions with nodes. Errors are recorded and then `p`
// is used as a threshold to see if a node is 'good'
func NewDecaying() *Decaying {
	return &Decaying{
		p:  0.0,
		e:  math.E,
		r:  math.Log(0.5) / 10,
		t0: time.Now(),
	}
}

// Value is the current value of this decaying error value - the time since it was
// created is stored for adjustment.
func (decaying *Decaying) Value() float64 {
	decaying.Lock()
	now := time.Now()
	dt := now.Sub(decaying.t0).Seconds()
	decaying.t0 = now
	curValP := decaying.p
	decaying.p = curValP * math.Pow(decaying.e, (decaying.r*dt))
	pOut := decaying.p
	decaying.Unlock()
	return pOut
}

// Add icnrements the `p` value of the decaying error object
func (decaying *Decaying) Add(d float64) {
	prevVal := decaying.Value()
	decaying.Lock()
	decaying.p = prevVal + d
	decaying.Unlock()
}
