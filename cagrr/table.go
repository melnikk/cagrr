package cagrr

// Repairs of table
func (t *Table) Repairs() []*Repair {
	return t.repairs
}

// Total repairs in table
func (t *Table) Total() int {
	return t.total
}

// SetRepairs to table
func (t *Table) SetRepairs(repairs []*Repair) {
	t.repairs = repairs
}

// SetTotal to table
func (t *Table) SetTotal(total int) {
	t.total = total
}
