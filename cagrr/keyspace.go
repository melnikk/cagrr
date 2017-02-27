package cagrr

// SetTables to keyspace
func (k *Keyspace) SetTables(tables []*Table) {
	k.tables = tables
}

// SetTotal to keyspace
func (k *Keyspace) SetTotal(total int) {
	k.total = total
}

// Tables of keyspace
func (k *Keyspace) Tables() []*Table {
	return k.tables
}

// Total repairs in keyspace
func (k *Keyspace) Total() int {
	return k.total
}
