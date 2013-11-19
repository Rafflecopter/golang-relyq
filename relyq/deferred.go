package relyq

import (
	// "github.com/garyburd/redigo/redis"
	// "github.com/Rafflecopter/golang-relyq/scripts"
	"time"
)

// Push a deferred task onto the queue
func (q *RelyQ) Defer(task Task, when time.Time) error {
	if !q.Cfg.AllowDefer {
		panic("Deferred tasks not allowed on this relyq")
	}

	//TODO
	return nil
}
