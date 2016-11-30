package main

import (
	"bytes"
	"testing"
)

func TestVersion(t *testing.T) {
	opts.Verbosity = "debug"
	opts.Version = true
	var buf bytes.Buffer
	out = &buf

	checkVersion()

	if buf.String() != version {
		t.Errorf("It should return version info (\"%s\" expected, %s returned)", version, buf.String())
	}
}
