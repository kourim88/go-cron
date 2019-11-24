package worker

import (
	"io/ioutil"
	"encoding/json"
)

type Config struct {
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeout int `json:"mongodbConnectTimeout"`
	TaskLogBatchSize int `json:"taskLogBatchSize"`
	TaskLogCommitTimeout int `json"taskLogCommitTimeout"`
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