package repair

import "github.com/skbkontur/cagrr/ops"

var (
	log       ops.Logger
	regulator ops.Regulator
)

// SetLogger sets package logger
func SetLogger(logger ops.Logger) {
	log = logger
}

// SetRegulator sets package regulator
func SetRegulator(reg ops.Regulator) {
	regulator = reg
}
