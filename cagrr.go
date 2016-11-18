package cagrr

// Config is a configuration file struct
type Config struct {
	Host     string          `yaml:"host"`
	Port     int             `yaml:"port"`
	Clusters []ClusterConfig `yaml:"clusters"`
}

// ClusterConfig contains configuration of cluster item
type ClusterConfig struct {
	ID        int
	Name      string     `yaml:"name"`
	Interval  string     `yaml:"interval"`
	Keyspaces []Keyspace `yaml:"keyspaces"`
}

// Keyspace contains keyspace repair schedule description
type Keyspace struct {
	Name   string  `yaml:"name"`
	Slices int     `yaml:"slices"`
	Tables []Table `yaml:"tables"`
}

// Table contains column families to repair
type Table struct {
	Name   string `yaml:"name"`
	Slices int    `yaml:"slices"`
}

// Token represents primary key range
type Token struct {
	ID     string `json:"id"`
	Ranges []Fragment
}

// Fragment of Token range for repair
type Fragment struct {
	ID       int `json:"id"`
	Endpoint string
	Start    string
	End      string
}

// RepairStatus keeps status of repair
type RepairStatus struct {
	ID       int
	Cluster  string
	Keyspace string
	Tables   string
	Count    int
	Duration int
	Error    bool
	Message  string
	Session  string
	Total    int
	Type     string
}

// ColumnFamily creates or returns existed table
func (k Keyspace) ColumnFamily(name string) Table {
	result := Table{name, k.Slices}
	for _, table := range k.Tables {
		if table.Name == name {
			result.Slices = table.Slices
		}
	}
	return result
}
