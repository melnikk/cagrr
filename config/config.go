package config

import (
	"io/ioutil"
	"path/filepath"

	"github.com/skbkontur/cagrr"

	"gopkg.in/yaml.v2"
)

// Reader reads configuration from sources
type Reader interface {
	Read(filename string) (Config, error)
}

// Config wraps app config
type Config cagrr.Config

var (
	reader Config
)

// CreateReader creates reader of configuration
func CreateReader() Reader {
	reader = Config{}
	result := Reader(reader)
	return result
}

func (c Config) Read(filename string) (Config, error) {

	filename, _ = filepath.Abs(filename)
	yamlFile, err := ioutil.ReadFile(filename)

	if err != nil {
		return c, err
	}

	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		return c, err
	}
	return c, nil
}
