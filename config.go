package cagrr

import (
	"io/ioutil"
	"path/filepath"

	yaml "gopkg.in/yaml.v2"
)

// Config is a configuration file struct
type Config struct {
	Clusters []ClusterConfig `yaml:"clusters"`
}

// ClusterConfig contains configuration of cluster item
type ClusterConfig struct {
	Host      string   `yaml:"host"`
	Port      int      `yaml:"port"`
	Interval  string   `yaml:"interval"`
	Keyspaces []string `yaml:"keyspaces"`
}

// ReadConfig reads configuration file into struct
func ReadConfig(filename string) (Config, error) {
	config := Config{}
	filename, _ = filepath.Abs(filename)
	yamlFile, err := ioutil.ReadFile(filename)

	if err != nil {
		return config, err
	}

	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		return config, err
	}
	return config, nil
}
