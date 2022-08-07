package cfg

import (
	"encoding/json"
	"ngin/libs/elastic"
	"ngin/libs/logger"
	"ngin/libs/mysql"
	"ngin/libs/redis"
	"os"
)

/*
  "elastic":{
        "common":{
            "host":"127.0.0.1",
            "port":9200,
            "username":"",
            "pwd":""
        }
    },
*/
type Config struct {
	// Mysql map[string]*MysqlCfg `json:"mysql"`
	Env     string                         `json:"env"`
	Elastic map[string]*elastic.ElasticCfg `json:"elastic"`
	Redis   map[string]*redis.RedisCfg     `json:"redis"`
	Logger  map[string][]*logger.LoggerCfg `json:"logger"`
	Mysql   map[string]*mysql.MysqlCfg     `json:"mysql"`
	RpcUrl  string                         `json:"json_rpc_url"`
	IsDebug bool                           `json:"-"`
}

func LoadCfg(path string) (*Config, error) {
	var cfg Config
	configFile, err := os.Open(path)
	if err == nil {
		defer configFile.Close()
	}
	if err != nil {
		return nil, err
	}
	jsonParser := json.NewDecoder(configFile)
	if err := jsonParser.Decode(&cfg); err != nil {
		return nil, err
	}
	if cfg.Env == "debug" {
		cfg.IsDebug = true
	}
	return &cfg, nil
}
