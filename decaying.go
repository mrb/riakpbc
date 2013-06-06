package riakpbc

import (
	"math"
	"time"
)

type Decaying struct {
	p  float64
	e  float64
	r  float64
	t0 time.Time
}

func NewDecaying() *Decaying {
	return &Decaying{
		p:  0.0,
		e:  math.E,
		r:  math.Log(0.5) / 10.0,
		t0: time.Now(),
	}
}

func (decaying *Decaying) Value() float64 {
	now := time.Now()
	dt := now.Sub(decaying.t0).Seconds() * 1000 * 1000
	decaying.t0 = now
	curValP := decaying.p
	decaying.p = curValP * math.Pow(decaying.e, (decaying.r*dt))
	return decaying.p
}

func (decaying *Decaying) Add(d float64) {
	prevVal := decaying.Value()
	decaying.p = prevVal + d
}
