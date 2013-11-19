// Package redis provides a Storage backend for relyq
package redisstorage

import (
	"github.com/Rafflecopter/golang-relyq/marshallers"
	"github.com/garyburd/redigo/redis"
)

type RedisStorage struct {
	pool   *redis.Pool
	m      marshallers.Marshaller
	prefix string
}

func New(marshaller marshallers.Marshaller, pool *redis.Pool, prefix, delim string) *RedisStorage {
	return &RedisStorage{
		pool:   pool,
		m:      marshaller,
		prefix: prefix + delim + "jobs" + delim,
	}
}

func (rs *RedisStorage) Get(id string) (map[string]interface{}, error) {
	val, err2 := redis.Bytes(rs.do("GET", rs.prefix+id))

	if err2 != nil {
		return nil, err2
	}

	return rs.m.Unmarshal(val)
}

func (rs *RedisStorage) Set(obj map[string]interface{}, id string) error {
	val, err := rs.m.Marshal(obj)
	if err != nil {
		return err
	}

	_, err = rs.do("SET", rs.prefix+id, val)
	return err
}

func (rs *RedisStorage) Del(id string) error {
	_, err := rs.do("DEL", rs.prefix+id)
	return err
}

func (rs *RedisStorage) Close() error {
	return nil
}

func (rs *RedisStorage) do(cmd string, args ...interface{}) (interface{}, error) {
	conn := rs.pool.Get()
	defer conn.Close()
	return conn.Do(cmd, args...)
}
