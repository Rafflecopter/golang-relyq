package relyq

import (
	"github.com/Rafflecopter/golang-relyq/marshallers"
	"github.com/Rafflecopter/golang-relyq/storage/redis"
	"github.com/garyburd/redigo/redis"
)

func NewRedisJson(pool *redis.Pool, cfg *Config) *RelyQ {
	cfg.Defaults()
	storage := redisstorage.New(marshallers.JSON, pool, cfg.Prefix, cfg.Delimiter)
	return New(pool, storage, cfg)
}
