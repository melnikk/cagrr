package repair

import "github.com/skbkontur/cagrr/ops"

// Runner starts fragment repair
type Runner interface {
	Run() error
	ops.Meter
}
