// Package relyq provides a reliable queue backed by redis
package relyq

import (
	"errors"
	"github.com/Rafflecopter/golang-simpleq/simpleq"
	"github.com/extemporalgenome/uuid"
	"github.com/garyburd/redigo/redis"
	"github.com/yanatan16/gowaiter"
	"io"
)

// A reliable redis-backed queue
type Queue struct {
	// The underlying simpleqs
	Todo, Doing, Done, Failed *simpleq.Queue
	Storage                   Storage
	Cfg                       *Config
	listener                  *Listener
}

// Configuration for Relyq
type Config struct {
	// Required: Prefix on simpleq.Queue key names for redis
	Prefix string
	// Field in any object which contains a unique identifier
	// Defaults to "id"
	IdField string
	// Redis delimeter. Defaults to ":"
	Delimiter string
	// Clean finish (i.e. no Done queue)
	// Defaults to false
	UseDoneQueue bool
	// Should we keep the task stored after they are done?
	// Defaults to false
	KeepDoneTasks bool
}

// A useful alias for a task
type Task map[string]interface{}

// Storage interface
type Storage interface {
	// Get a task object
	Get(taskid string) (map[string]interface{}, error)
	// Save a task object
	Set(task map[string]interface{}, taskid string) error
	// Delete the task object in the storage
	Del(taskid string) error
	// End the Storage connection
	io.Closer
}

// Create a reliable queue
func New(pool *redis.Pool, storage Storage, cfg *Config) *Queue {
	cfg.Defaults()

	rq := &Queue{
		Todo:    simpleq.New(pool, cfg.Prefix+cfg.Delimiter+"todo"),
		Doing:   simpleq.New(pool, cfg.Prefix+cfg.Delimiter+"doing"),
		Failed:  simpleq.New(pool, cfg.Prefix+cfg.Delimiter+"failed"),
		Storage: storage,
		Cfg:     cfg,
	}

	if cfg.UseDoneQueue {
		rq.Done = simpleq.New(pool, cfg.Prefix+cfg.Delimiter+"done")
	}

	return rq
}

// Push a task onto the queue
func (q *Queue) Push(task Task) error {
	id := q.id(task)
	w := waiter.New(2)

	go func() {
		if err := q.Storage.Set(task, id); err != nil {
			w.Errors <- err
		}
		w.Done <- true
	}()

	go func() {
		if _, err := q.Todo.Push([]byte(id)); err != nil {
			w.Errors <- err
		}
		w.Done <- true
	}()

	return w.Wait()
}

// Move the next task to the Doing queue. May return nil
func (q *Queue) Process() (Task, error) {
	id, err := q.Todo.PopPipe(q.Doing)
	if err != nil {
		return nil, err
	}

	t, err := q.Storage.Get(string(id))
	return Task(t), err
}

// Block and process the next task.
func (q *Queue) BProcess(timeout_secs int) (Task, error) {
	id, err := q.Todo.BPopPipe(q.Doing, timeout_secs)
	if err != nil {
		return nil, err
	}

	t, err := q.Storage.Get(string(id))
	return Task(t), err
}

// Move a task to the Done queue if in use
// If a task is not in use, delete if CleanFinishKeepStorage is false
// Sometimes a task is in the Failed queue already (maybe timeout) so we check there if not in Finish
func (q *Queue) Finish(task Task) error {
	id := q.id(task)
	w := waiter.New(2)

	go func() {
		if q.Cfg.KeepDoneTasks {
			if err := q.Storage.Set(map[string]interface{}(task), id); err != nil {
				w.Errors <- err
			}
		} else {
			if err := q.Storage.Del(id); err != nil {
				w.Errors <- err
			}
		}
		w.Done <- true
	}()

	go func() {
		if q.Cfg.UseDoneQueue {
			if n, err := q.Doing.SPullPipe(q.Done, []byte(id)); err != nil {
				w.Errors <- err
			} else if n == 0 {
				if n, err := q.Failed.SPullPipe(q.Done, []byte(id)); err != nil {
					w.Errors <- err
				} else if n == 0 {
					w.Errors <- errors.New("Task " + id + " not found in Doing or Failed queues.")
				}
			}
		} else {
			if n, err := q.Doing.Pull([]byte(id)); err != nil {
				w.Errors <- err
			} else if n == 0 {
				if n, err := q.Failed.Pull([]byte(id)); err != nil {
					w.Errors <- err
				} else if n == 0 {
					w.Errors <- errors.New("Task " + id + " not found in Doing or Failed queues.")
				}
			}
		}
		w.Done <- true
	}()

	return w.Wait()
}

// Move a task to the Failed queue
func (q *Queue) Fail(task Task) error {
	id := q.id(task)
	w := waiter.New(2)

	go func() {
		if err := q.Storage.Set(map[string]interface{}(task), id); err != nil {
			w.Errors <- err
		}
		w.Done <- true
	}()

	go func() {
		if n, err := q.Doing.SPullPipe(q.Failed, []byte(id)); err != nil {
			w.Errors <- err
		} else if n == 0 {
			w.Errors <- errors.New("Task " + id + " not found in Doing queue.")
		}
		w.Done <- true
	}()

	return w.Wait()
}

// Remove a task from a queue
// If dontDelete (single extra arg) is true, then no delete call will be done for the task
func (q *Queue) Remove(subq *simpleq.Queue, task Task, keepInStorage ...bool) error {
	id := q.id(task)
	w := waiter.New(2)

	go func() {
		if len(keepInStorage) > 0 && keepInStorage[0] {
			if err := q.Storage.Set(map[string]interface{}(task), id); err != nil {
				w.Errors <- err
			}
		} else {
			if err := q.Storage.Del(id); err != nil {
				w.Errors <- err
			}
		}
		w.Done <- true
	}()

	go func() {
		if n, err := subq.Pull([]byte(id)); err != nil {
			w.Errors <- err
		} else if n == 0 {
			w.Errors <- errors.New("Task " + id + " not found in queue.")
		}
		w.Done <- true
	}()

	return w.Wait()
}

// End the queue
func (q *Queue) Close() error {
	w := waiter.New(5)

	w.Close(q.Todo)
	w.Close(q.Doing)
	w.Close(q.Failed)
	w.Close(q.Done)
	w.Close(q.Storage)

	return w.Wait()
}

func (q *Queue) id(task Task) string {
	if id, ok := task[q.Cfg.IdField]; ok {
		if sid, ok := id.(string); ok {
			return sid
		}
	}

	id := uuid.NewRandom().String()
	task[q.Cfg.IdField] = id
	return id
}

// Remove the ID field from
func (q *Queue) resetId(task Task) string {
	delete(task, q.Cfg.IdField)
	return q.id(task)
}

func (cfg *Config) Defaults() {
	if cfg.Prefix == "" {
		panic("Prefix required for relyq")
	}

	if cfg.IdField == "" {
		cfg.IdField = "id"
	}

	if cfg.Delimiter == "" {
		cfg.Delimiter = ":"
	}
}
