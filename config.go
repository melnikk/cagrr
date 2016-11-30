package cagrr

import (
	"io/ioutil"
	"path/filepath"

	"gopkg.in/yaml.v1"
)

// ReadConfiguration parses yaml configuration file
func ReadConfiguration(filename string) (*Config, error) {
	var c Config
	filename, _ = filepath.Abs(filename)
	yamlFile, err := ioutil.ReadFile(filename)

	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(yamlFile, &c)
	return &c, err
}
