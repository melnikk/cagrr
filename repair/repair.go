package repair

import "github.com/skbkontur/cagrr/ops"

var (
	log        ops.Logger
	regulators map[string]ops.Regulator
)

func init() {
	regulators = make(map[string]ops.Regulator)
}

// SetLogger sets package logger
func SetLogger(logger ops.Logger) {
	log = logger
}

func setRegulator(clusterName string, reg ops.Regulator) {
	regulators[clusterName] = reg
}

func getRegulator(clusterName string) ops.Regulator {
	return regulators[clusterName]
}
