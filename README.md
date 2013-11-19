# relyq [![Build Status][1]][2]

A relatively simple Redis-backed reliable task queue and state machine.

Its made up of four [simpleq](https://github.com/Rafflecopter/golang-simpleq)'s: todo, doing, failed, and done. Tasks will never be dropped on the floor even if a processing server crashes because all operations are atomic. Tasks can be represented as any data type.

_Note_: relyq assumes all tasks are objects. It is opinionated in that way. Also, all tasks have id's. If they don't exist, they are created using a random uuid.

There are a few redis clients for Go but this package uses [redigo](https://github.com/garyburd/redigo)

## Install

```
go get github.com/Rafflecopter/golang-relyq/relyq
```

## Creation

```go
import (
  "github.com/garyburd/redigo/redis"
  "github.com/Rafflecopter/golang-relyq/relyq"
  redisstorage "github.com/Rafflecopter/golang-relyq/storage/redis"
)

func CreateRelyQ(pool *redis.Pool) *relyq.RelyQ {
  cfg := &relyq.Config{
    Prefix: "my-relyq", // Required
    Delimiter: ":", // Defaults to :
    IdField: "id", // ID field for tasks
    UseDoneQueue: false, // Whether to keep list of "done" tasks (default false)
    KeepDoneTasks: false, // Whether to keep the backend storage of "done" tasks (default false)
  }

  storage := redisstorage.New(redisstorage.JSONMarshaller, pool, cfg.Prefix, cfg.Delimiter)

  return relyq.New(pool, storage, cfg)
}

func QuickCreateRelyQ(pool *redis.Pool) *relyq.RelyQ {
  return redisstorage.NewJson(pool, &relyq.Config{Prefix: "my-relyq"})
}
```

## Use

Basic use:

```go
q := CreateRelyQ(redisPool)

// Push a task on to Todo queue
err := q.Push(Task{"some":"fields","a":1})

// Move a task from Todo to Doing
task, err := q.Process()

// Block and process a task
task, err := q.BProcess()

// Finish a task
err := q.Finish(task)

// Or fail it
err := q.Fail(task)

// Remove a task from the Failed queue
err := q.Remove(q.Failed, task)

// Eventually
err := q.Close()
```

Or use a listener:

```go
q := CreateRelyQ(redisPool)

l := q.Listen()

go func() {
  for err := range l.Errors {
    // Do something with errors
  }
}()

go func() {
  for task := range l.Tasks {
    // Do something with tasks
    err := q.Finish(task)
  }
}()

// Eventually
err := q.Close()
```

## Tests

```
go test
```

## Storage Options

Normal operation stores the full task description or object in the queue itself. This can be inefficient for LREM operations. Sometimes one might even want to store task descriptions in a separate datastore than redis to save memory. Custom backends have been created for this purpose.

### Redis

The Redis backend stores serialized task objects in Redis. Each options object also accepts the `storage_prefix` field to set the prefix for where task objects are stored. [Documentation](http://godoc.org/github.com/Rafflecopter/golang-relyq/storage/redis)

Create a storage object:

```go
storage := redisstorage.New(redisstorage.JSONMarshaller, pool, cfg.Prefix, cfg.Delimiter)
```

Or, for skipping the storage step, use this handy shortcut
```go
q := relyq.NewRedisJson(pool, cfg)
```

## TODO

- deferred tasks
- recurring tasks

## License

See LICENSE file.

[1]: https://travis-ci.org/Rafflecopter/golang-relyq.png?branch=master
[2]: http://travis-ci.org/Rafflecopter/golang-relyq