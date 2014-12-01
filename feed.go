package syncedfeed

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/fzzy/radix/redis"
)

type Feed struct {
	*sync.Mutex
	Name string

	Messages map[int64]*Message

	Client   *redis.Client
	PSClient *redis.Client
}

type Message struct {
	Id   int64
	Body string
}

func NewFeed(name string, addr string) (f *Feed, err error) {
	f.Client, err = redis.DialTimeout("tcp", addr, 30*time.Second)

	if err != nil {
		return nil, err
	}

	return f, nil
}

func (f *Feed) publish(action string, id int64) error {
	msg := action + ":" + f.toStr(id)
	f.Client.Cmd("PUBLISH", f.Name, msg)
	return nil
}

func (f *Feed) toStr(id int64) string {
	return strconv.FormatInt(id, 10)
}

func (f *Feed) getMessage(id int64) (m *Message, err error) {
	reply := f.Client.Cmd("ZRANGE", f.Name, id, id+1)
	ret, err := reply.List()
	if err != nil {
		return
	}

	if len(ret) == 0 {
		err = errors.New("Could not find message: " + f.toStr(id))
		return
	}

	body := ret[0]
	m.Id = id
	m.Body = body
	return
}

func (f *Feed) push(id int64) (err error) {
	m, err := f.getMessage(id)
	f.Messages[m.Id] = m
	return
}

func (f *Feed) update(id int64) (err error) {
	m, err := f.getMessage(id)
	f.Messages[m.Id] = m
	return
}

func (f *Feed) Push(body string) (m *Message, err error) {
	f.Lock()
	defer f.Unlock()

	m.Id = time.Now().UnixNano()
	m.Body = body
	err = f.Client.Cmd("ZADD", f.Name, m.Id, m.Body).Err
	if err != nil {
		return
	}

	err = f.publish("push", m.Id)
	return
}

func (f *Feed) Remove(id int64) (err error) {
	f.Lock()
	defer f.Unlock()

	err = f.Client.Cmd("ZREM", f.Name, id).Err
	if err != nil {
		return
	}

	err = f.publish("remove", id)
	return
}

func (f *Feed) Update(id int64, body string) (err error) {
	f.Lock()
	defer f.Unlock()

	err = f.Client.Cmd("ZADD", f.Name, id, body).Err
	if err != nil {
		return
	}

	err = f.publish("update", id)
	return
}
