package relyq

import (
	"github.com/Rafflecopter/golang-simpleq/simpleq"
	"github.com/yanatan16/errorcaller"
)

type Listener struct {
	l                   *simpleq.Listener
	Errors              chan error
	Tasks, Fail, Finish chan Task
	rq                  *RelyQ
	closeErrorCount     int
}

// Start a listener
func (q *RelyQ) Listen() *Listener {
	if q.listener == nil {
		q.listener = NewListener(q, q.Todo.PopPipeListen(q.Doing))
	}
	return q.listener
}

func NewListener(rq *RelyQ, sql *simpleq.Listener) *Listener {
	l := &Listener{
		l:      sql,
		Tasks:  make(chan Task),
		Fail:   make(chan Task),
		Finish: make(chan Task),
		Errors: make(chan error),
		rq:     rq,
	}

	go l.listenOnError()
	go l.listenOnFinish()
	go l.listenOnElements()
	return l
}

func (l *Listener) Close() error {
	return l.l.Close()
}

func (l *Listener) listenOnError() {
	for err := range l.l.Errors {
		l.Errors <- errorcaller.Err(err)
	}
	l.closeErrors()
}

func (l *Listener) listenOnFinish() {
	defer l.closeErrors()
	for {
		select {
		case t, ok := <-l.Fail:
			if !ok {
				return
			}
			if err := l.rq.Fail(t); err != nil {
				l.Errors <- errorcaller.Err(err)
			}
		case t, ok := <-l.Finish:
			if !ok {
				return
			}
			if err := l.rq.Finish(t); err != nil {
				l.Errors <- errorcaller.Err(err)
			}
		}
	}
}

func (l *Listener) listenOnElements() {
	defer func() {
		l.closeErrors()
		close(l.Tasks)
	}()

	for id := range l.l.Elements {
		if task, err := l.rq.Storage.Get(string(id)); err != nil {
			l.Errors <- errorcaller.Err(err)
		} else {
			l.Tasks <- task
		}
	}
}

func (l *Listener) closeErrors() {
	l.closeErrorCount += 1
	if l.closeErrorCount == 3 {
		close(l.Errors)
	}
}
