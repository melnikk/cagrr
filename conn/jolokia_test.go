// +build integration

package conn

import "testing"

func TestRing(t *testing.T) {
	runner := NewRunner("172.16.238.10", "8080")
	runner.Ring()
}
