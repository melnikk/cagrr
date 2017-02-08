package cagrr

// Repairs of table
func (t *Table) Repairs() []*Repair {
	return t.repairs
}

// SetRepairs to table
func (t *Table) SetRepairs(repairs []*Repair) {
	t.repairs = repairs
}
