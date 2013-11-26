package scripts

import "github.com/garyburd/redigo/redis"

// Move tasks from a deferred zset to the todo simpleq
var DeferMove = redis.NewScript(
	2, // KEYS:[deferred_zset, todo_simpleq], ARGV:[now]
	`local refs = redis.call("zrangebyscore", KEYS[1], 0, ARGV[1])
  if table.getn(refs) > 0 then
    redis.call("lpush", KEYS[2], unpack(refs))
    redis.call("zremrangebyscore", KEYS[1], 0, ARGV[1])
  end
  return refs`)
