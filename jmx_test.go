package cagrr

import "testing"

func TestRing(t *testing.T) {
	runner := NewRunner("127.0.0.1", "7199")
	runner.Ring()
}
