package cagrr

import "github.com/boltdb/bolt"

var (
	database     DB
	tablesNeeded = []string{clusterRepairs, currentPositions, savedPositions}
)

// NewDb connects to DB
func NewDb(name string) DB {
	instance := boltDB{}
	var err error
	instance.db, err = bolt.Open(name, 0600, nil)
	if err != nil {
		log.Error(err)
	}
	instance.CreateTables()
	return &instance
}

func (d *boltDB) Close() {
	d.db.Close()
}

// CreateTables initializes tables needed for work
func (d *boltDB) CreateTables() error {
	tx, err := d.db.Begin(true)
	if err != nil {
		log.WithError(err).Warn("Error when starting transaction")
		return err
	}
	defer tx.Rollback()
	for _, table := range tablesNeeded {
		_, err := tx.CreateBucketIfNotExists([]byte(table))
		if err != nil {
			log.WithError(err).Warn("Error when creating bucket")
			return err
		}
	}
	tx.Commit()
	return nil
}

func (d *boltDB) ReadOrCreate(table, key, defaultValue string) string {
	val := d.ReadValue(table, key)
	if val == "" {
		d.WriteValue(table, key, defaultValue)
		val = defaultValue
	}
	return val
}

func (d *boltDB) ReadValue(table, key string) string {
	var result string
	tx, err := d.db.Begin(false)
	if err != nil {
		log.WithError(err).Warn("Error when starting read transaction")
		return result
	}
	defer tx.Rollback()

	b := tx.Bucket([]byte(table))
	//if b != nil {
	result = string(b.Get([]byte(key)))
	//}

	return result
}

// SetDatabase sets package-level DB interface
func SetDatabase(db DB) {
	database = db
}

func (d *boltDB) WriteValue(table, key, value string) error {
	tx, err := d.db.Begin(true)
	if err != nil {
		log.WithError(err).Warn("Error when starting write transaction")
		return err
	}
	defer tx.Rollback()

	b, _ := tx.CreateBucketIfNotExists([]byte(table))

	b.Put([]byte(key), []byte(value))

	// Commit the transaction.
	if err := tx.Commit(); err != nil {
		log.WithError(err).Warn("Error when commiting write transaction")
		return err
	}

	return nil
}

func getDatabase() DB {
	return database
}
