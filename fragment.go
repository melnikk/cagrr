package cagrr

func (frag *Fragment) createRepair(table *Table, callback string) *Repair {

	res := Repair{
		ID:       frag.ID,
		Start:    frag.Start,
		End:      frag.End,
		Endpoint: frag.Endpoint,
		Cluster:  frag.cluster,
		Keyspace: frag.keyspace,
		Table:    table.Name,
		Callback: callback,
	}
	return &res
}
