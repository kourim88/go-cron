package master

import (
	"io/ioutil"
	"encoding/json"
)

type Config struct {
	ApiPort int	`json:"apiPort"`
	ApiReadTimeout int	`json:"apiReadTimeout"`
	ApiWriteTimeout int	`json:"apiWriteTimeout"`
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	WebRoot string `json:"webroot"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int `json:"mongodbConnectTimeout"`
}

var (
	Sg_config *Config
)

func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf Config
	)

	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	Sg_config = &conf

	return
}