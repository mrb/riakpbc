package riakpbc

import (
	"github.com/bmizerany/assert"
	"testing"
	"time"
)

func TestDecaying(t *testing.T) {
	now := time.Now()
	decaying := NewDecaying()
	assert.T(t, decaying != nil)
	assert.T(t, decaying.t0.After(now))
	assert.T(t, decaying.Value() == 0.0)
	decaying.Add(1.0)
	assert.T(t, decaying.Value() < 1.0)
	decaying.Add(6.0)
	lastVal := decaying.Value()
	assert.T(t, lastVal > 5.0)
	trulyLastVal := decaying.Value()
	assert.T(t, lastVal > trulyLastVal)
}
