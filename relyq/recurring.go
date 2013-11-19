package relyq

import (
	// "github.com/garyburd/redigo/redis"
	// "github.com/Rafflecopter/golang-relyq/scripts"
	"time"
)

// Push a recurring task onto the queue
func (q *RelyQ) Recur(task Task, every time.Duration) error {
	if !q.Cfg.AllowRecur {
		panic("Recurring tasks not allowed on this relyq")
	}
	//TODO

	return nil
}
