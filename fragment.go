package cagrr

import "strconv"

const (
	currentPositions = "CurrentPositions"
	savedPositions   = "SavedPositions"
)

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

func (frag *Fragment) currentPosition() int {
	db := getDatabase()
	val := db.ReadOrCreate(currentPositions, frag.cluster, "0")

	result, _ := strconv.Atoi(val)
	return result
}

func (frag *Fragment) incrementCurrentPosition() int {
	position := frag.currentPosition() + 1

	frag.position = position

	db := getDatabase()
	db.WriteValue(currentPositions, frag.cluster, strconv.Itoa(position))

	return position
}

func (frag *Fragment) loadPosition() int {
	db := getDatabase()
	val := db.ReadOrCreate(savedPositions, frag.cluster, "0")

	result, _ := strconv.Atoi(val)

	return result
}

func (frag *Fragment) needToRepair() bool {
	current := frag.incrementCurrentPosition()
	saved := frag.loadPosition()
	return current > saved
}

func (frag *Fragment) savePosition() {
	result := strconv.Itoa(frag.position)
	db := getDatabase()
	db.WriteValue(savedPositions, frag.cluster, result)
}
