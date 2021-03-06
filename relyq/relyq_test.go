package relyq

import (
	"fmt"
	"github.com/Rafflecopter/golang-relyq/marshallers"
	"github.com/Rafflecopter/golang-relyq/storage/redis"
	"github.com/Rafflecopter/golang-simpleq/simpleq"
	"github.com/garyburd/redigo/redis"
	"io"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

var _ = fmt.Println

var pool *redis.Pool

func init() {
	rand.Seed(time.Now().Unix())
	pool = redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial("tcp", ":6379")
	}, 10)
}

// -- Tests --

func TestPush(t *testing.T) {
	q := begin(nil, defaultConfig())
	defer end(t, q)

	push(t, q, ArbitraryTask{"f": "foo123"})
	push(t, q, ArbitraryTask{"f": "456bar"})

	checkTaskList(t, q, q.Todo, ArbitraryTask{"f": "456bar"}, ArbitraryTask{"f": "foo123"})
}

func TestProcess(t *testing.T) {
	q := begin(nil, defaultConfig())
	defer end(t, q)

	push(t, q, ArbitraryTask{"f": "pleasure"})
	push(t, q, ArbitraryTask{"f": "pain"})

	tp := ArbitraryTask{}
	if ok, err := q.Process(&tp); err != nil {
		t.Error("Process", err)
	} else if !ok {
		t.Error("Process returned nil")
	} else {
		checkTaskEqual(t, tp, ArbitraryTask{"f": "pleasure"})
	}

	checkTaskList(t, q, q.Todo, ArbitraryTask{"f": "pain"})
	checkTaskList(t, q, q.Doing, ArbitraryTask{"f": "pleasure"})
}

func TestStructProcess(t *testing.T) {
	q := begin(nil, defaultConfig())
	defer end(t, q)

	push(t, q, &TaskStruct{F: "pleasure"})
	push(t, q, &TaskStruct{F: "pain"})

	tp := new(TaskStruct)
	if ok, err := q.Process(tp); err != nil {
		t.Error("Process", err)
	} else if !ok {
		t.Error("Process returned nil!")
	} else {
		checkTaskStructEqual(t, tp, &TaskStruct{F: "pleasure"})
	}

	checkTaskStructList(t, q, q.Todo, &TaskStruct{F: "pain"})
	checkTaskStructList(t, q, q.Doing, &TaskStruct{F: "pleasure"})
}

func TestBProcess(t *testing.T) {
	q := begin(nil, defaultConfig())
	defer end(t, q, q)

	go func() {
		push(t, q, ArbitraryTask{"f": "happy-days"})
		push(t, q, ArbitraryTask{"f": "television"})
		push(t, q, ArbitraryTask{"f": "shows"})
	}()

	tp := ArbitraryTask{}
	if err := q.BProcess(1, &tp); err != nil {
		t.Error("BProcess", err)
	} else {
		checkTaskEqual(t, tp, ArbitraryTask{"f": "happy-days"})
	}

	tp = ArbitraryTask{}
	if err := q.BProcess(1, &tp); err != nil {
		t.Error("BProcess", err)
	} else {
		checkTaskEqual(t, tp, ArbitraryTask{"f": "television"})
	}

	checkTaskList(t, q, q.Todo, ArbitraryTask{"f": "shows"})
	checkTaskList(t, q, q.Doing, ArbitraryTask{"f": "television"}, ArbitraryTask{"f": "happy-days"})
}

func TestFinish(t *testing.T) {
	cfg := defaultConfig()
	cfg.UseDoneQueue = true
	cfg.KeepDoneTasks = true
	q := begin(nil, cfg)
	defer end(t, q)

	push(t, q, ArbitraryTask{"f": "something"})
	push(t, q, ArbitraryTask{"f": "else"})
	push(t, q, ArbitraryTask{"f": "argument"})

	tp := ArbitraryTask{}
	if ok, err := q.Process(&tp); !ok || err != nil {
		t.Error("Process", ok, err)
	} else {
		checkTaskEqual(t, tp, ArbitraryTask{"f": "something"})
		tp["result"] = "another-thing"

		t.Log(tp)

		if err := q.Finish(tp); err != nil {
			t.Error("Finish", err)
		}
	}

	tp = ArbitraryTask{}
	if ok, err := q.Process(&tp); err != nil || !ok {
		t.Error("Process", ok, err)
	} else {
		checkTaskEqual(t, tp, ArbitraryTask{"f": "else"})
	}

	checkTaskList(t, q, q.Todo, ArbitraryTask{"f": "argument"})
	checkTaskList(t, q, q.Doing, ArbitraryTask{"f": "else"})
	checkTaskList(t, q, q.Done, ArbitraryTask{"f": "something", "result": "another-thing"})
}

func TestCleanFinish(t *testing.T) {
	q := begin(nil, defaultConfig())
	defer end(t, q)

	push(t, q, ArbitraryTask{"f": "something"})
	push(t, q, ArbitraryTask{"f": "else"})
	push(t, q, ArbitraryTask{"f": "argument"})

	tp := ArbitraryTask{}
	if ok, err := q.Process(&tp); err != nil || !ok {
		t.Error("Process", ok, err)
	} else {
		checkTaskEqual(t, tp, ArbitraryTask{"f": "something"})

		if err := q.Finish(tp); err != nil {
			t.Error("Finish", err)
		}
	}

	tp = ArbitraryTask{}
	if ok, err := q.Process(&tp); !ok || err != nil {
		t.Error("Process", ok, err)
	} else {
		checkTaskEqual(t, tp, ArbitraryTask{"f": "else"})
	}

	checkTaskList(t, q, q.Todo, ArbitraryTask{"f": "argument"})
	checkTaskList(t, q, q.Doing, ArbitraryTask{"f": "else"})
}

func TestFail(t *testing.T) {
	q := begin(nil, defaultConfig())
	defer end(t, q)

	push(t, q, ArbitraryTask{"f": "try"})
	push(t, q, ArbitraryTask{"f": "new"})
	push(t, q, ArbitraryTask{"f": "things"})

	tp := ArbitraryTask{}
	if ok, err := q.Process(&tp); !ok || err != nil {
		t.Error("Process", ok, err)
	} else {
		checkTaskEqual(t, tp, ArbitraryTask{"f": "try"})
	}

	if ok, err := q.Process(&tp); !ok || err != nil {
		t.Error("Process", ok, err)
	} else {
		checkTaskEqual(t, tp, ArbitraryTask{"f": "new"})

		if err := q.Fail(tp); err != nil {
			t.Error("Fail", err)
		}
	}

	checkTaskList(t, q, q.Todo, ArbitraryTask{"f": "things"})
	checkTaskList(t, q, q.Doing, ArbitraryTask{"f": "try"})
	checkTaskList(t, q, q.Failed, ArbitraryTask{"f": "new"})
}

func TestFinishFail(t *testing.T) {
	q := begin(nil, defaultConfig())
	defer end(t, q)

	push(t, q, ArbitraryTask{"a": 1})
	push(t, q, ArbitraryTask{"a": 2})
	push(t, q, ArbitraryTask{"a": 3})
	push(t, q, ArbitraryTask{"a": 4})

	savetp := ArbitraryTask{}
	tp := ArbitraryTask{}

	if ok, err := q.Process(&savetp); err != nil || !ok {
		t.Error("Process", ok, err)
	} else {
		checkTaskEqual(t, savetp, ArbitraryTask{"a": float64(1)})

		if err := q.Fail(savetp); err != nil {
			t.Error("Fail", err)
		}
	}

	tp = ArbitraryTask{}
	if ok, err := q.Process(&tp); !ok || err != nil {
		t.Error("Process", ok, err)
	} else {
		checkTaskEqual(t, tp, ArbitraryTask{"a": float64(2)})

		if err := q.Fail(tp); err != nil {
			t.Error("Fail", err)
		}
	}

	if err := q.Finish(savetp); err != nil {
		t.Error("FailFinish", err)
	}

	tp = ArbitraryTask{}
	if ok, err := q.Process(&tp); !ok || err != nil {
		t.Error("Process", ok, err)
	} else {
		checkTaskEqual(t, tp, ArbitraryTask{"a": float64(3)})
	}

	checkTaskList(t, q, q.Todo, ArbitraryTask{"a": float64(4)})
	checkTaskList(t, q, q.Doing, ArbitraryTask{"a": float64(3)})
	checkTaskList(t, q, q.Failed, ArbitraryTask{"a": float64(2)})
}

func TestRemove(t *testing.T) {
	q := begin(nil, defaultConfig())
	defer end(t, q)

	push(t, q, ArbitraryTask{"f": "foo"})
	push(t, q, ArbitraryTask{"f": "bar", "id": "123"}) // Known ID

	tp := ArbitraryTask{}
	if ok, err := q.Process(&tp); !ok || err != nil {
		t.Error("Process", ok, err)
	} else {
		checkTaskEqual(t, tp, ArbitraryTask{"f": "foo"})

		if err := q.Remove(q.Doing, tp); err != nil {
			t.Error("Remove", err)
		}
	}

	if err := q.Remove(q.Todo, ArbitraryTask{"f": "bar", "id": "123"}); err != nil {
		t.Error("Remove", err)
	}

	checkTaskList(t, q, q.Todo)
	checkTaskList(t, q, q.Doing)
}

func TestListen(t *testing.T) {
	q := begin(nil, defaultConfig())
	defer end(t, q)
	done, clsd := make(chan bool), make(chan bool)

	push(t, q, ArbitraryTask{"x": "1"})
	push(t, q, ArbitraryTask{"x": "2"})
	push(t, q, ArbitraryTask{"x": "3"})

	var example ArbitraryTask
	l := q.Listen(example)

	go func() {
		for err := range l.Errors {
			t.Error("Listener", err)
		}
		clsd <- true
	}()

	go func() {
		for itask := range l.Tasks {
			task := itask.(ArbitraryTask)

			switch task["x"] {
			case "1":
				l.Finish <- task
			case "2":
				task["y"] = "2"
				l.Fail <- task
			case "3":
				done <- true
			}
		}

		clsd <- true
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Error("Timeout waiting for done")
		return
	}

	checkTaskList(t, q, q.Todo)
	checkTaskList(t, q, q.Doing, ArbitraryTask{"x": "3"})
	checkTaskList(t, q, q.Failed, ArbitraryTask{"x": "2", "y": "2"})

	if err := l.Close(); err != nil {
		t.Error(err)
	}

	close(l.Finish)
	close(l.Fail)

	for i := 0; i < 2; i++ {
		select {
		case <-clsd:
		case <-time.After(50 * time.Millisecond):
			t.Error("Timeout on closing!", i)
		}
	}
}

func TestStructListen(t *testing.T) {
	q := begin(nil, defaultConfig())
	defer end(t, q)
	done, clsd := make(chan bool), make(chan bool)

	push(t, q, &TaskStruct{F: "1"})
	push(t, q, &TaskStruct{F: "2"})
	push(t, q, &TaskStruct{F: "3"})

	var example *TaskStruct
	l := q.Listen(example)

	go func() {
		for err := range l.Errors {
			t.Error("Listener", err)
		}
		clsd <- true
	}()

	go func() {
		for itask := range l.Tasks {
			task := itask.(*TaskStruct)

			switch task.F {
			case "1":
				l.Finish <- task
			case "2":
				task.G = "2"
				l.Fail <- task
			case "3":
				done <- true
			}
		}

		clsd <- true
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Error("Timeout waiting for done")
		return
	}

	checkTaskStructList(t, q, q.Todo)
	checkTaskStructList(t, q, q.Doing, &TaskStruct{F: "3"})
	checkTaskStructList(t, q, q.Failed, &TaskStruct{F: "2", G: "2"})

	if err := l.Close(); err != nil {
		t.Error(err)
	}

	close(l.Finish)
	close(l.Fail)

	for i := 0; i < 2; i++ {
		select {
		case <-clsd:
		case <-time.After(50 * time.Millisecond):
			t.Error("Timeout on closing!", i)
		}
	}
}

// -- Helpers --

type TaskStruct struct {
	StructuredTask
	F, G string
}

func defaultConfig() *Config {
	return &Config{
		Prefix: randKey(),
	}
}

func randKey() string {
	return "go-relyq-test:" + rstr(8)
}

func rstr(n int) string {
	s := make([]byte, 8)
	for i := 0; i < n; i++ {
		s[i] = byte(rand.Int()%26 + 97)
	}
	return string(s)
}

func checkTaskList(t *testing.T, rq *Queue, sq *simpleq.Queue, els ...ArbitraryTask) {
	list, err := sq.List()
	if err != nil {
		t.Error("Error List(): " + err.Error())
	}
	if len(list) == 0 && len(els) == 0 {
		return
	}
	if len(list) != len(els) {
		t.Error("List isn't the same length as els")
		return
	}

	for i, id := range list {
		el := ArbitraryTask{}
		err := rq.Storage.Get(id, &el)
		if err != nil {
			t.Error("Error doing storage.Get(): ", err, string(id))
			continue
		}

		checkTaskEqual(t, el, els[i])
	}
}

func checkTaskStructList(t *testing.T, rq *Queue, sq *simpleq.Queue, els ...*TaskStruct) {
	list, err := sq.List()
	if err != nil {
		t.Error("Error List(): " + err.Error())
	}
	if len(list) == 0 && len(els) == 0 {
		return
	}
	if len(list) != len(els) {
		t.Error("List isn't the same length as els")
		return
	}

	for i, id := range list {
		el := new(TaskStruct)
		err := rq.Storage.Get(id, el)
		if err != nil {
			t.Error("Error doing storage.Get(): ", err, string(id))
			continue
		}

		checkTaskStructEqual(t, el, els[i])
	}
}

func checkTaskEqual(t *testing.T, el, compare ArbitraryTask) {
	if id, ok := el["id"]; !ok || id == nil {
		t.Error("element has no id!", el)
	} else {
		compare["id"] = el["id"]
		if !reflect.DeepEqual(el, compare) {
			t.Error("List element isn't as it should be:", el, compare)
		}
	}
}

func checkTaskStructEqual(t *testing.T, el, compare *TaskStruct) {
	if el == nil {
		t.Error("element is nil!")
		return
	} else if el.RqId == "" {
		t.Error("element has no id!", el)
	} else {
		compare.RqId = el.RqId
		if !reflect.DeepEqual(el, compare) {
			t.Error("Task structs are not equal!", el, compare)
		}
	}
}

func basicStorage(prefix string) Storage {
	return redisstorage.New(marshallers.Json, pool, prefix, ":")
}

func begin(s Storage, c *Config) *Queue {
	if s == nil {
		s = basicStorage(c.Prefix)
	}
	q := New(pool, s, c)
	q.Todo.Clear()
	q.Doing.Clear()
	q.Failed.Clear()
	if q.Done != nil {
		q.Done.Clear()
	}
	return q
}

func end(t *testing.T, qs ...io.Closer) {
	for _, q := range qs {
		if err := q.Close(); err != nil {
			t.Error(err)
		}
	}
	t.Log("Active redis connections:", pool.ActiveCount())
}

func push(t *testing.T, q *Queue, task Ider) {
	if err := q.Push(task); err != nil {
		t.Error("Error Push(", task, "): ", err)
	}
}
