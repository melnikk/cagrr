package cagrr

// SetTables to keyspace
func (k *Keyspace) SetTables(tables []*Table) {
	k.tables = tables
}

// Tables of keyspace
func (k *Keyspace) Tables() []*Table {
	return k.tables
}
