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

func (rs *RedisStorage) Get(id []byte, obj interface{}) error {
	val, err := redis.Bytes(rs.do("GET", rs.prefixed(id)))

	if err != nil {
		return err
	}

	return rs.m.Unmarshal(val, obj)
}

func (rs *RedisStorage) Set(obj interface{}, id []byte) error {
	val, err := rs.m.Marshal(obj)
	if err != nil {
		return err
	}

	_, err = rs.do("SET", rs.prefixed(id), val)
	return err
}

func (rs *RedisStorage) Del(id []byte) error {
	_, err := rs.do("DEL", rs.prefixed(id))
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

func (rs *RedisStorage) prefixed(id []byte) []byte {
	return append([]byte(rs.prefix), id...)
}
