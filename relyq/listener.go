package relyq

import (
	"github.com/Rafflecopter/golang-simpleq/simpleq"
	"github.com/yanatan16/errorcaller"
	"reflect"
)

type Listener struct {
	l                   *simpleq.Listener
	Errors              chan error
	Tasks, Fail, Finish chan Ider
	rq                  *Queue
	closeErrorCount     int
}

// Start a listener
func (q *Queue) Listen(example Ider) *Listener {
	if q.listener == nil {
		q.listener = NewListener(q, q.Todo.PopPipeListen(q.Doing), example)
	}
	return q.listener
}

func NewListener(rq *Queue, sql *simpleq.Listener, example Ider) *Listener {
	l := &Listener{
		l:      sql,
		Tasks:  make(chan Ider),
		Fail:   make(chan Ider),
		Finish: make(chan Ider),
		Errors: make(chan error),
		rq:     rq,
	}

	go l.listenOnError()
	go l.listenOnFinish()
	go l.listenOnElements(example)
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

func (l *Listener) listenOnElements(example Ider) {
	defer func() {
		l.closeErrors()
		close(l.Tasks)
	}()

	typ := reflect.TypeOf(example)
	isPointer := false

	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
		isPointer = true
	}

	for id := range l.l.Elements {
		task := reflect.New(typ).Interface()
		if err := l.rq.Storage.Get(id, task); err != nil {
			l.Errors <- errorcaller.Err(err)
		} else {

			if !isPointer {
				task = reflect.ValueOf(task).Elem().Interface()
			}

			l.Tasks <- task.(Ider)
		}
	}
}

func (l *Listener) closeErrors() {
	l.closeErrorCount += 1
	if l.closeErrorCount == 3 {
		close(l.Errors)
	}
}
