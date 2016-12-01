package cagrr

import (
	"time"
)

// RegisterStart sets start time of keyspace repair
func (k *Keyspace) RegisterStart(tables []*Table) {
	k.percent = 0
	k.Tables = tables
}

// RegisterFinish sets value of successful keyspace repair
func (k *Keyspace) RegisterFinish() {
	k.percent = 100
}

func (k *Keyspace) findTable(name string) *Table {
	for i, t := range k.Tables {
		if t.Name == name {
			return k.Tables[i]
		}
	}
	panic("Table not found")
}

func (k *Keyspace) percentage() int32 {
	if k.percent == 100 {
		return k.percent
	}

	result := int32(0)
	for _, t := range k.Tables {
		result += t.percentage()
	}
	result = result / int32(len(k.Tables))
	return result
}

func (k *Keyspace) estimate() time.Duration {
	result := 0
	for _, t := range k.Tables {
		result += int(t.estimate())
	}
	result = result / len(k.Tables)
	return time.Duration(result)
}
